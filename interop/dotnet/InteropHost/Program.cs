// Interop harness host (WP A4): drives the real C# RedisEvents library from the command line.
// Every verb prints JSON lines on stdout (logs go to stderr). Exit codes:
//   0 ok | 1 usage or unexpected failure | 2 timeout (fewer than --expect messages)
//   3 not implemented (rust-host only) | 4 the library refused (config / codec / shrink guard)
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisEvents.Admin;
using RedisEvents.Extensions;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Producer;
using RedisEvents.Wire;
using StackExchange.Redis;

try
{
    return await Cli.RunAsync(args);
}
catch (CliException e)
{
    Cli.Emit(new() { ["event"] = "error", ["kind"] = "usage", ["message"] = e.Message });
    return 1;
}
catch (StreamConfigurationException e)
{
    Cli.Emit(new() { ["event"] = "refused", ["message"] = e.Message });
    return 4;
}
catch (Exception e)
{
    Cli.Emit(new() { ["event"] = "error", ["kind"] = e.GetType().Name, ["message"] = e.Message });
    return 1;
}

internal sealed class CliException(string message) : Exception(message);

internal sealed class Args
{
    private readonly Dictionary<string, List<string>> values = new(StringComparer.Ordinal);
    private readonly HashSet<string> flags = new(StringComparer.Ordinal);

    public string Verb { get; }

    public Args(string[] argv)
    {
        if (argv.Length == 0) throw new CliException("missing verb");
        Verb = argv[0];
        for (var i = 1; i < argv.Length; i++)
        {
            var a = argv[i];
            if (!a.StartsWith("--", StringComparison.Ordinal)) throw new CliException($"unexpected argument '{a}'");
            var name = a[2..];
            if (name is "batch" or "buffered" or "seq" or "no-warmup") { flags.Add(name); continue; }
            if (i + 1 >= argv.Length) throw new CliException($"--{name} needs a value");
            if (!values.TryGetValue(name, out var list)) values[name] = list = [];
            list.Add(argv[++i]);
        }
    }

    public bool Flag(string n) => flags.Contains(n);
    public IReadOnlyList<string> All(string n) => values.TryGetValue(n, out var l) ? l : [];
    public string? Opt(string n) => values.TryGetValue(n, out var l) ? l[^1] : null;
    public string Req(string n) => Opt(n) ?? throw new CliException($"--{n} is required");
    public int Int(string n, int dflt) => Opt(n) is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : dflt;
}

internal static class Cli
{
    private static readonly JsonSerializerOptions Json = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
    private static readonly object Gate = new();

    public static void Emit(Dictionary<string, object?> line)
    {
        lock (Gate) Console.Out.WriteLine(JsonSerializer.Serialize(line, Json));
        Console.Out.Flush();
    }

    public static async Task<int> RunAsync(string[] argv)
    {
        var a = new Args(argv);
        return a.Verb switch
        {
            "publish" => await Publish(a),
            "consume" => await Consume(a),
            "positions" => await Positions(a),
            "admin-reset" => await AdminReset(a),
            "meta" => await Meta(a),
            _ => throw new CliException($"unknown verb '{a.Verb}'"),
        };
    }

    private static string RedisEndpoint(Args a)
    {
        var url = a.Opt("redis") ?? Environment.GetEnvironmentVariable("REDIS_URL") ?? "redis://localhost:6379";
        if (!url.Contains("://", StringComparison.Ordinal)) return url;
        var u = new Uri(url);
        return $"{u.Host}:{(u.Port > 0 ? u.Port : 6379)}";
    }

    private static HostApplicationBuilder NewBuilder(Args a, string topic, int partitions, string? consumer)
    {
        var b = Host.CreateApplicationBuilder([]);
        b.Logging.ClearProviders();
        b.Logging.AddConsole(o => o.LogToStandardErrorThreshold = LogLevel.Trace);
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        var cfg = new Dictionary<string, string?>
        {
            ["Streams:ConnectionString"] = RedisEndpoint(a) + ",abortConnect=false",
            [$"Streams:Topics:{topic}:Partitions"] = partitions.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (consumer is not null)
        {
            cfg["Streams:Consumers:0:Topic"] = topic;
            cfg["Streams:Consumers:0:Consumer"] = consumer;
            cfg["Streams:Consumers:0:PersistIntervalMs"] = "100";
        }
        b.Configuration.AddInMemoryCollection(cfg);
        return b;
    }

    private static async Task<int> Publish(Args a)
    {
        var topic = a.Req("topic");
        var keys = a.All("key");
        if (keys.Count == 0) throw new CliException("--key is required");
        var type = a.Req("type");
        var body = Convert.FromBase64String(a.Opt("body-b64") ?? "");
        var count = a.Int("count", 1);
        var buffered = a.Flag("buffered");
        var batch = a.Flag("batch");
        var headers = a.All("header").Select(h =>
        {
            var i = h.IndexOf('=', StringComparison.Ordinal);
            if (i < 0) throw new CliException($"--header needs k=v, got '{h}'");
            return new KeyValuePair<string, string>(h[..i], h[(i + 1)..]);
        }).ToList();
        var opts = new PublishOptions(a.Opt("correlation"), headers.Count > 0 ? headers : null);

        var builder = NewBuilder(a, topic, a.Int("partitions", 1), null);
        builder.AddStreamPublisher(topic, buffered);
        using var host = builder.Build();
        await host.StartAsync();

        // Trace context rides in the entry's `p` field, stamped from Activity.Current.
        Activity? activity = null;
        if (a.Opt("traceparent") is { } tp)
        {
            activity = new Activity("interop.publish");
            activity.SetIdFormat(ActivityIdFormat.W3C);
            activity.SetParentId(tp);
            activity.Start();
        }

        // A cold buffered publisher reorders its first flush: every pipelined PublishAsync awaits
        // EnsureTopicAsync, and the continuations race. So by default the topic is ensured first;
        // --no-warmup exposes the library's behaviour (scenario buffered_publish_cold_order).
        if (buffered && !a.Flag("no-warmup"))
        {
            using var warm = (ConnectionMultiplexer)await Connect(a);
            await StreamAdmin.EnsureTopicAsync(warm, topic, new TopicOptions { Partitions = a.Int("partitions", 1) });
        }

        var pub = host.Services.GetRequiredKeyedService<IStreamPublisher>(topic);
        var bufPub = buffered ? host.Services.GetRequiredKeyedService<IStreamBufferedPublisher>(topic) : null;
        var seq = a.Flag("seq");

        ReadOnlyMemory<byte> BodyFor(string key, int i) =>
            seq ? Encoding.UTF8.GetBytes($"{Encoding.UTF8.GetString(body)}|{key}|{i}") : body;

        var published = 0;
        if (batch)
        {
            foreach (var key in keys)
            {
                var bodies = Enumerable.Range(0, count).Select(i => BodyFor(key, i)).ToArray();
                await pub.PublishBatchAsync(key, bodies, type, opts);
                published += count;
            }
        }
        else
        {
            for (var i = 0; i < count; i++)
            {
                foreach (var key in keys)
                {
                    if (bufPub is not null) await bufPub.EnqueueAsync(key, BodyFor(key, i), type, opts);
                    else
                    {
                        var id = await pub.PublishAsync(key, BodyFor(key, i), type, opts);
                        Emit(new() { ["event"] = "published", ["key"] = key, ["id"] = id.ToString() });
                    }
                    published++;
                }
            }
        }

        if (bufPub is not null) await bufPub.FlushAsync();
        activity?.Stop();
        await host.StopAsync();
        Emit(new() { ["event"] = "done", ["published"] = published });
        return 0;
    }

    private static async Task<int> Consume(Args a)
    {
        var topic = a.Req("topic");
        var consumer = a.Req("consumer");
        var expect = a.Int("expect", 1);
        var timeout = TimeSpan.FromSeconds(a.Int("timeout", 30));
        var seen = 0;
        var reached = new TaskCompletionSource();
        if (expect <= 0) reached.TrySetResult();

        var builder = NewBuilder(a, topic, a.Int("partitions", 1), consumer);
        builder.AddStream(topic, (batch, _) =>
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var m = batch.Span[i];
                var hs = new List<string[]>();
                foreach (var h in m.Headers)
                    hs.Add([Encoding.UTF8.GetString(h.KeyUtf8.Span), Encoding.UTF8.GetString(h.ValueUtf8.Span)]);
                Emit(new()
                {
                    ["event"] = "message",
                    ["partition"] = m.Partition,
                    ["id"] = m.Id.ToString(),
                    ["key"] = m.PartitionKey,
                    ["type"] = m.Type,
                    ["body_b64"] = Convert.ToBase64String(m.Body.Span),
                    ["headers"] = hs,
                    ["correlation"] = m.CorrelationId,
                    ["traceparent"] = m.TraceParent,
                });
                if (Interlocked.Increment(ref seen) >= expect && expect > 0) reached.TrySetResult();
            }
            return ValueTask.CompletedTask;
        });

        using var host = builder.Build();
        await host.StartAsync();
        var winner = await Task.WhenAny(reached.Task, Task.Delay(timeout));
        // Stopping flushes the consumer's positions, which the resume scenarios rely on.
        await host.StopAsync();
        var timedOut = winner != reached.Task;
        Emit(new() { ["event"] = "done", ["consumed"] = seen, ["timed_out"] = timedOut && expect > 0 });
        return timedOut && expect > 0 ? 2 : 0;
    }

    private static async Task<IConnectionMultiplexer> Connect(Args a) =>
        await ConnectionMultiplexer.ConnectAsync(RedisEndpoint(a) + ",abortConnect=false");

    private static async Task<int> Positions(Args a)
    {
        var topic = a.Req("topic");
        var consumer = a.Req("consumer");
        using var redis = (ConnectionMultiplexer)await Connect(a);
        var entries = await redis.GetDatabase().HashGetAllAsync($"{KeyNamespace.Prefix()}p:{{{topic}}}:{consumer}");
        var positions = new SortedDictionary<int, string>();
        var markers = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var e in entries)
        {
            var name = e.Name.ToString();
            var value = e.Value.ToString();
            if (int.TryParse(name, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var p))
                positions[p] = value.Split('|')[0];
            else markers[name] = value;
        }
        Emit(new() { ["event"] = "positions", ["positions"] = positions.ToDictionary(k => k.Key.ToString(System.Globalization.CultureInfo.InvariantCulture), k => k.Value), ["markers"] = markers });
        return 0;
    }

    private static async Task<int> AdminReset(Args a)
    {
        var topic = a.Req("topic");
        var consumer = a.Req("consumer");
        var to = StreamId.Parse(a.Req("to-id"));
        int? partition = a.Opt("partition") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : null;
        using var redis = (ConnectionMultiplexer)await Connect(a);
        var res = await StreamAdmin.ResetPositionAsync(redis, topic, consumer, to, partition);
        Emit(new()
        {
            ["event"] = "reset",
            ["targets"] = res.Select(r => new Dictionary<string, object> { ["partition"] = r.Partition, ["id"] = r.Target.ToString() }).ToList(),
        });
        return 0;
    }

    private static async Task<int> Meta(Args a)
    {
        var topic = a.Req("topic");
        using var redis = (ConnectionMultiplexer)await Connect(a);
        var entries = await redis.GetDatabase().HashGetAllAsync($"{KeyNamespace.Prefix()}m:{{{topic}}}");
        Emit(new() { ["event"] = "meta", ["fields"] = entries.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString()) });
        return 0;
    }
}
