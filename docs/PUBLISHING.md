# Publishing to crates.io

Every push to `main` that passes `test` publishes the six library crates (`redisevents`,
`-msgpack`, `-projections`, `-restate`, `-axum`, `-eventsourcing`), in that dependency order, from the
`publish` job in `.github/workflows/ci.yml`. It mirrors the C# repo's NuGet job.

## Versioning (same as the C# repo)

`version.json` holds `major.minor` (Nerdbank.GitVersioning style); the patch is the git height, the
number of commits since `version.json` last changed. `scripts/set_version.sh` computes
`<major.minor>.<height>` and rewrites the workspace version in the CI checkout only. The committed
`Cargo.toml` stays at `<major.minor>.0`. To start a new minor, edit `version.json` and the
`version = "0.2.0"` requirements on the internal path dependencies (the script rewrites those in CI).

## Authentication

The job tries crates.io Trusted Publishing (OIDC, `rust-lang/crates-io-auth-action`,
`id-token: write`) and falls back to the `CARGO_REGISTRY_TOKEN` secret. Re-runs are idempotent: a
crate whose version is already on crates.io is skipped, and an existing `v<version>` tag is kept.
The job runs in the `production` GitHub environment, as in the C# repo; create it (Settings,
Environments) or remove the `environment:` line.

## One-time bootstrap

Trusted Publishing can only be configured for a crate that already exists, so the first release of
each crate must use a token:

1. Create a crates.io API token with the `publish-new` and `publish-update` scopes.
2. Add it as the repository (or `production` environment) secret `CARGO_REGISTRY_TOKEN`.
3. Merge to `main` (or re-run the `CI` workflow on main). All six crates are published.
   Alternatively publish manually, in order, from a checkout of main:
   `v=$(scripts/set_version.sh) && CARGO_REGISTRY_TOKEN=... scripts/publish_crates.sh "$v"`
4. For each of the six crates on crates.io: Settings, Trusted Publishing, add a GitHub publisher:
   owner `bklooste`, repository `RedisEvents-rs`, workflow `ci.yml`, environment `production`.
5. Delete the `CARGO_REGISTRY_TOKEN` secret. Later releases use OIDC alone.
