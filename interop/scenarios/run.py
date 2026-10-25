#!/usr/bin/env python3
"""Cross-language interop scenario runner (WP A4).

Drives the two host binaries (C# InteropHost, Rust interop-rust-host) through the JSON-lines CLI and
asserts on what comes back, plus on the raw Redis entries it reads itself with a tiny RESP client.
Python 3 stdlib only: it needs no toolchain beyond what a CI runner already has, and it keeps the
orchestration (two processes of two languages, raw Redis) independent of either implementation under
test. A Rust runner would have had to pull a Redis client into the workspace and would share code
with the thing it checks.

Exit code 3 from a host means "verb not implemented yet": the scenario/pair is SKIPPED, not failed.
Exit status of this script: 0 when nothing failed, 1 otherwise.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import subprocess
import sys
import time
import traceback
import uuid
from collections import defaultdict
from urllib.parse import urlparse

NOT_IMPLEMENTED = 3
REFUSED = 4
TIMEOUT = 2


class Skip(Exception):
    """A host returned exit code 3 (verb not implemented yet)."""


# --------------------------------------------------------------------------------- raw Redis
class Resp:
    """Minimal RESP2 client, enough for HGETALL / XRANGE / SET / SCAN / DEL."""

    def __init__(self, url: str) -> None:
        u = urlparse(url)
        self.sock = socket.create_connection((u.hostname or "localhost", u.port or 6379), timeout=10)
        self.f = self.sock.makefile("rb")

    def call(self, *args: str | bytes | int):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._read()

    def _read(self):
        line = self.f.readline().rstrip(b"\r\n")
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:-2]
            return data
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError(f"bad RESP line {line!r}")

    def hgetall(self, key: str) -> dict[str, bytes]:
        flat = self.call("HGETALL", key) or []
        return {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}

    def xrange(self, key: str) -> list[tuple[str, dict[str, bytes]]]:
        res = self.call("XRANGE", key, "-", "+") or []
        return [
            (eid.decode(), {f[i].decode(): f[i + 1] for i in range(0, len(f), 2)})
            for eid, f in res
        ]

    def delete_prefix(self, prefix: str) -> None:
        cursor = b"0"
        while True:
            cursor, keys = self.call("SCAN", cursor, "MATCH", prefix + "*", "COUNT", 500)
            if keys:
                self.call("DEL", *keys)
            if cursor == b"0":
                break


# --------------------------------------------------------------------------------- hosts
class Host:
    def __init__(self, lang: str, argv: list[str], env: dict[str, str]) -> None:
        self.lang, self.argv, self.env = lang, argv, env

    def run(self, verb: str, *args: str, timeout: int = 90, allow: tuple[int, ...] = (0,)):
        """Run one verb. Returns (rc, json_lines). Raises Skip on 3, AssertionError on other rc."""
        cmd = [*self.argv, verb, *args]
        p = subprocess.run(cmd, env=self.env, capture_output=True, text=True, timeout=timeout)
        lines = []
        for ln in p.stdout.splitlines():
            ln = ln.strip()
            if ln.startswith("{"):
                lines.append(json.loads(ln))
        if p.returncode == NOT_IMPLEMENTED:
            raise Skip(f"{self.lang} {verb}")
        if p.returncode not in allow:
            raise AssertionError(
                f"{self.lang} {verb} exited {p.returncode} (allowed {allow})\n"
                f"  cmd: {' '.join(cmd)}\n  stdout: {p.stdout[-800:]}\n  stderr: {p.stderr[-800:]}"
            )
        return p.returncode, lines


def b64(s: str | bytes) -> str:
    return base64.b64encode(s.encode() if isinstance(s, str) else s).decode()


def pack_headers(pairs: list[tuple[str, str]]) -> bytes:
    """WIRE.md section 5, written independently of both implementations."""
    out = b""
    for k, v in pairs:
        kb, vb = k.encode(), v.encode()
        out += b"%d:%s%d:%s" % (len(kb), kb, len(vb), vb)
    return out


# --------------------------------------------------------------------------------- context
class Ctx:
    def __init__(self, hosts: dict[str, Host], redis: Resp, prefix: str, producer: str, consumer: str, tag: str):
        self.hosts, self.redis, self.prefix = hosts, redis, prefix
        self.p, self.c = hosts[producer], hosts[consumer]
        self.tag = tag

    def topic(self, name: str) -> str:
        return f"{self.tag}-{name}"

    def messages(self, lines):
        return [m for m in lines if m.get("event") == "message"]

    def stream_key(self, topic: str, p: int) -> str:
        return f"{self.prefix}s:{{{topic}}}:{p}"

    def publish(self, host: Host, topic: str, partitions: int, keys: list[str], *extra: str, count: int = 1,
                body: str = "m", typ: str = "Interop.Msg"):
        args = ["--topic", topic, "--partitions", str(partitions), "--type", typ,
                "--body-b64", b64(body), "--count", str(count)]
        for k in keys:
            args += ["--key", k]
        return host.run("publish", *args, *extra)

    def consume(self, host: Host, topic: str, consumer: str, partitions: int, expect: int, timeout: int = 20,
                allow: tuple[int, ...] = (0,)):
        rc, lines = host.run("consume", "--topic", topic, "--consumer", consumer, "--partitions", str(partitions),
                             "--expect", str(expect), "--timeout", str(timeout), allow=allow)
        return rc, self.messages(lines)


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


# Gate vocabulary: "pair" scenarios take their gate from the direction of the (producer, consumer)
# pair (rust->cs is G1, cs->rust is G2; same-language pairs are the baseline). Others name one.
SCENARIOS: list[tuple[str, str, object]] = []
XFAIL: dict[str, str] = {}  # scenario name -> reason; a known C# library bug, reported but not failing.
# It applies only when C# is the producer: a Rust producer passing says nothing about the C# bug.
SHARED: dict[str, object] = {}  # facts the first pair establishes (C# is the reference), others must match


def scenario(name: str, gate: str = "pair", xfail: str = ""):
    def deco(fn):
        SCENARIOS.append((name, gate, fn))
        if xfail:
            XFAIL[name] = xfail
        return fn
    return deco


def gate_for(gate: str, producer: str, consumer: str) -> str:
    if gate != "pair":
        return gate
    return {("rust", "cs"): "G1", ("cs", "rust"): "G2"}.get((producer, consumer), "baseline")


# --------------------------------------------------------------------------------- scenarios
def _keyed(ctx: Ctx, n: int):
    topic = ctx.topic(f"keyed{n}")
    keys = [f"key-{i}" for i in range(8)]
    ctx.publish(ctx.p, topic, n, keys, "--seq", count=5)
    _, msgs = ctx.consume(ctx.c, topic, "g", n, 40)
    check(len(msgs) == 40, f"expected 40 messages, got {len(msgs)}")
    placement: dict[str, int] = {}
    order: dict[str, list[int]] = defaultdict(list)
    for m in msgs:
        k = m["key"]
        check(placement.setdefault(k, m["partition"]) == m["partition"], f"key {k} split across partitions")
        _, bk, i = base64.b64decode(m["body_b64"]).decode().split("|")
        check(bk == k, "body/key mismatch")
        order[k].append(int(i))
    for k, seq in order.items():
        check(seq == list(range(5)), f"per-key order broken for {k}: {seq}")
    # The partition the consumer reports is the stream the entry really sits in.
    for k, part in placement.items():
        entries = ctx.redis.xrange(ctx.stream_key(topic, part))
        check(sum(1 for _, f in entries if f["k"].decode() == k) == 5, f"key {k} not in stream {part}")
    check(all(p < n for p in placement.values()), "partition out of range")
    # Every pair must route identically to the first pair that ran (C# -> C#, the reference).
    ref = SHARED.setdefault(f"keyed{n}", placement)
    check(ref == placement, f"routing differs from reference: {ref} vs {placement}")


for _n in (1, 4, 7, 16):
    scenario(f"keyed_publish_{_n}_partitions")(lambda ctx, n=_n: _keyed(ctx, n))


@scenario("headers_correlation_traceparent")
def headers_roundtrip(ctx: Ctx):
    topic = ctx.topic("hdr")
    headers = [("tenant", "acme"), ("a:b", "c:d"), ("n", "12:34"), ("ünï", "日本語"), ("empty", "")]
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    args = ["--correlation", "corr-€-1", "--traceparent", tp]
    for k, v in headers:
        args += ["--header", f"{k}={v}"]
    ctx.publish(ctx.p, topic, 1, ["hk"], *args)
    _, [m] = ctx.consume(ctx.c, topic, "g", 1, 1)
    check([tuple(h) for h in m["headers"]] == headers, f"headers: {m['headers']}")
    check(m["correlation"] == "corr-€-1", f"correlation: {m['correlation']}")
    check(m["traceparent"] and m["traceparent"].split("-")[1] == tp.split("-")[1], f"trace id lost: {m['traceparent']}")
    # Raw wire check, independent of both libraries.
    [(_, f)] = ctx.redis.xrange(ctx.stream_key(topic, 0))
    check(f["h"] == pack_headers(headers), f"packed headers: {f['h']!r}")
    check(f["c"].decode() == "corr-€-1" and f["t"].decode() == "Interop.Msg", "raw c/t fields")
    check(f["p"].decode().split("-")[1] == tp.split("-")[1], "raw p trace id")
    check(f["b"] == b"m", "raw body")


@scenario("optional_fields_omitted")
def omitted(ctx: Ctx):
    topic = ctx.topic("omit")
    ctx.publish(ctx.p, topic, 1, ["k"])
    _, [m] = ctx.consume(ctx.c, topic, "g", 1, 1)
    check(m["headers"] == [] and m["correlation"] in ("", None), "phantom metadata")
    [(_, f)] = ctx.redis.xrange(ctx.stream_key(topic, 0))
    check(set(f) == {"b", "t", "k"}, f"optional fields must be omitted, entry has {sorted(f)}")


@scenario("unicode_empty_long_keys")
def unicode_keys(ctx: Ctx):
    topic = ctx.topic("uni")
    keys = ["ключ-🔑", "日本語キー", "é", "é", "x" * 1000, "k:with:colons", "  spaced  "]
    n = 7
    ctx.publish(ctx.p, topic, n, keys, "--seq", count=3)
    _, msgs = ctx.consume(ctx.c, topic, "g", n, 3 * len(keys))
    placement: dict[str, int] = {}
    for m in msgs:
        check(placement.setdefault(m["key"], m["partition"]) == m["partition"], f"key {m['key'][:12]!r} split")
    check(set(placement) == set(keys), "a key was altered in flight")
    for k, part in placement.items():
        raw = {f["k"].decode() for _, f in ctx.redis.xrange(ctx.stream_key(topic, part))}
        check(k in raw, f"raw k field missing for {k[:12]!r}")
    ref = SHARED.setdefault("unicode", placement)
    check(ref == placement, f"unicode routing differs from reference: {ref} vs {placement}")
    # Empty key: round-robin, but the `k` field is still present and empty.
    topic2 = ctx.topic("emptykey")
    ctx.publish(ctx.p, topic2, 4, [""], count=4)
    _, msgs = ctx.consume(ctx.c, topic2, "g", 4, 4)
    check(all(m["key"] == "" for m in msgs), "empty key not preserved")
    for p in range(4):
        for _, f in ctx.redis.xrange(ctx.stream_key(topic2, p)):
            check("k" in f and f["k"] == b"", "empty key must still write k")


def _bulk(ctx: Ctx, mode: str, *extra: str):
    topic = ctx.topic(mode + ("cold" if extra else ""))
    keys = ["alpha", "beta", "gamma"]
    ctx.publish(ctx.p, topic, 4, keys, f"--{mode}", "--seq", *extra, count=50)
    _, msgs = ctx.consume(ctx.c, topic, "g", 4, 150)
    check(len(msgs) == 150, f"count {len(msgs)}")
    for k in keys:
        seq = [int(base64.b64decode(m["body_b64"]).decode().split("|")[2]) for m in msgs if m["key"] == k]
        check(seq == list(range(50)), f"order for {k}: {seq[:10]}...")


scenario("batch_publish")(lambda ctx: _bulk(ctx, "batch"))
scenario("buffered_publish")(lambda ctx: _bulk(ctx, "buffered"))
scenario(
    "buffered_publish_cold_order",
    xfail="BufferedStreamPublisher reorders the first flush of a cold publisher "
    "(each pipelined PublishAsync awaits EnsureTopicAsync); to be reported upstream",
)(lambda ctx: _bulk(ctx, "buffered", "--no-warmup"))


@scenario("positions_resume")
def resume(ctx: Ctx):
    topic = ctx.topic("resume")
    keys = ["r1", "r2", "r3"]
    # The producer's language consumes the first half and stops; the other language resumes.
    ctx.publish(ctx.p, topic, 4, keys, "--seq", count=4)
    _, first = ctx.consume(ctx.p, topic, "g", 4, 12)
    check(len(first) == 12, "first consumer short")
    _, [pos] = ctx.p.run("positions", "--topic", topic, "--consumer", "g")
    last = {}
    for m in first:
        last[str(m["partition"])] = m["id"]
    check(pos["positions"] == last, f"stored positions {pos['positions']} != last consumed {last}")
    _, [pos2] = ctx.c.run("positions", "--topic", topic, "--consumer", "g")
    check(pos2["positions"] == pos["positions"], "the other language reads different positions")
    ctx.publish(ctx.p, topic, 4, keys, "--seq", count=4, body="m2")
    _, second = ctx.consume(ctx.c, topic, "g", 4, 12)
    check(len(second) == 12, f"resumed consumer got {len(second)}, expected 12 (gap or replay)")
    check({m["id"] for m in first}.isdisjoint(m["id"] for m in second), "replay beyond the last flush")
    _, extra = ctx.consume(ctx.c, topic, "g", 4, 0, timeout=2)
    check(extra == [], "unexpected extra messages")


@scenario("admin_reset", gate="G3")
def admin_reset(ctx: Ctx):
    # Here the pair is (admin language, consumer language); C# publishes and does the first read.
    topic = ctx.topic("reset")
    cs = ctx.hosts["cs"]
    ctx.publish(cs, topic, 1, ["only"], "--seq", count=6)
    _, first = ctx.consume(cs, topic, "g", 1, 6)
    check(len(first) == 6, "first read short")
    target = first[2]["id"]
    ctx.p.run("admin-reset", "--topic", topic, "--consumer", "g", "--partition", "0", "--to-id", target)
    _, [pos] = ctx.c.run("positions", "--topic", topic, "--consumer", "g")
    check(pos["positions"].get("0") == target, f"reset position not written: {pos}")
    check(any(k.startswith("__reset") for k in pos["markers"]), f"reset marker not written: {pos}")
    # Cold start applies the marker; reading resumes strictly after the target.
    _, again = ctx.consume(ctx.c, topic, "g", 1, 3)
    check([m["id"] for m in again] == [m["id"] for m in first[3:]], "wrong messages after reset")
    _, [pos] = ctx.c.run("positions", "--topic", topic, "--consumer", "g")
    check(not pos["markers"], f"marker not cleared after apply: {pos['markers']}")


@scenario("partition_grow_refuse_shrink")
def grow(ctx: Ctx):
    topic = ctx.topic("grow")
    ctx.publish(ctx.p, topic, 4, ["a"])
    ctx.publish(ctx.p, topic, 8, ["a"])  # grow: allowed
    _, [meta] = ctx.c.run("meta", "--topic", topic)
    check(meta["fields"]["partitions"] == "8", f"grow not recorded: {meta}")
    rc, _ = ctx.c.run("publish", "--topic", topic, "--partitions", "4", "--key", "a", "--type", "t",
                      "--body-b64", b64("x"), allow=(REFUSED,))
    rc, _ = ctx.c.run("consume", "--topic", topic, "--consumer", "g", "--partitions", "4", "--expect", "0",
                      "--timeout", "2", allow=(REFUSED,))
    _, [meta] = ctx.c.run("meta", "--topic", topic)
    check(meta["fields"]["partitions"] == "8", "shrink must not change meta")


@scenario("codec_version_guard")
def codec_guard(ctx: Ctx):
    topic = ctx.topic("codec")
    ctx.publish(ctx.p, topic, 1, ["a"])
    check(ctx.redis.call("HSET", f"{ctx.prefix}m:{{{topic}}}", "v", "2") == 0, "v field not there to bump")
    rc, lines = ctx.c.run("consume", "--topic", topic, "--consumer", "g", "--partitions", "1", "--expect", "1",
                          "--timeout", "5", allow=(REFUSED,))
    check(not ctx.messages(lines), "a v=2 topic must not be decoded")
    rc, lines = ctx.p.run("publish", "--topic", topic, "--partitions", "1", "--key", "a", "--type", "t",
                          "--body-b64", b64("x"), allow=(REFUSED,))


# --------------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--redis", default=os.environ.get("REDIS_URL", "redis://localhost:6379"))
    ap.add_argument("--csharp", required=True, help="command for the C# host, e.g. 'dotnet path/InteropHost.dll'")
    ap.add_argument("--rust", required=True, help="path of the Rust host binary")
    ap.add_argument("--only", action="append", default=[], help="substring filter on scenario names")
    ap.add_argument("--gate", action="append", default=[], help="only run these gates (G1..G5, baseline)")
    ap.add_argument("--list", action="store_true", help="list scenarios and exit")
    a = ap.parse_args()

    if a.list:
        for name, gate, _ in SCENARIOS:
            print(f"{gate:8} {name}")
        return 0

    run_id = uuid.uuid4().hex[:8]
    prefix = f"iop{run_id}:"
    env = {**os.environ, "REDIS_URL": a.redis, "STREAMS_KEY_PREFIX": prefix}
    hosts = {"cs": Host("cs", a.csharp.split(), env), "rust": Host("rust", [a.rust], env)}
    redis = Resp(a.redis)

    # Reference language first so the shared routing facts come from C#.
    pairs = [("cs", "cs"), ("rust", "cs"), ("cs", "rust"), ("rust", "rust")]
    results = defaultdict(int)
    failed = False
    try:
        for name, gate, fn in SCENARIOS:
            if a.only and not any(s in name for s in a.only):
                continue
            for prod, cons in pairs:
                g = gate_for(gate, prod, cons)
                if a.gate and g not in a.gate:
                    continue
                label = f"[{g:8}] {name} ({prod}->{cons})"
                tag = f"{run_id}-{name[:14]}-{prod[0]}{cons[0]}".replace("_", "")
                ctx = Ctx(hosts, redis, prefix, prod, cons, tag)
                t0 = time.time()
                xfail = name in XFAIL and prod == "cs"
                try:
                    fn(ctx)
                    status = "XPASS  (known bug no longer reproduces; drop the xfail)" if xfail else "PASS"
                except Skip as s:
                    status = f"SKIP  (not implemented: {s})"
                except Exception as e:  # noqa: BLE001 - report every failure
                    if xfail:
                        status = "XFAIL  (known C# bug: " + XFAIL[name].split(";")[0] + ")"
                        print(f"{label} {status}\n    {str(e).splitlines()[0]}")
                        results["XFAIL"] += 1
                        continue
                    status = "FAIL"
                    failed = True
                    print(f"{label} FAIL\n    {str(e).replace(chr(10), chr(10) + '    ')}")
                    if not isinstance(e, AssertionError):
                        traceback.print_exc()
                results[status.split()[0]] += 1
                if status != "FAIL":
                    print(f"{label} {status} ({time.time() - t0:.1f}s)")
                sys.stdout.flush()
    finally:
        redis.delete_prefix(prefix)

    print("\nsummary: " + ", ".join(f"{k}={v}" for k, v in sorted(results.items())))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
