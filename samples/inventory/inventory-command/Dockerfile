# The Inventory write side. Build from the repository root (the workspace needs every member's manifest):
#   docker build -f samples/inventory/inventory-command/Dockerfile -t redisevents-inventory-command:local .
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p inventory-command

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 app
COPY --from=build /src/target/release/inventory-command /usr/local/bin/inventory-command
ENV PORT=8080
EXPOSE 8080
USER app
# Exec form: the service is PID 1 and gets SIGTERM directly, so it shuts down gracefully.
ENTRYPOINT ["/usr/local/bin/inventory-command"]
