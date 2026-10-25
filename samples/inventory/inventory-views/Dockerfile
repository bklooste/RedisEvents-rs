# Build context is the repository root: the crate uses workspace path dependencies.
#   docker build -f samples/inventory/inventory-views/Dockerfile -t redisevents-inventory-views:local .
FROM rust:slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p inventory-views

FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/inventory-views /usr/local/bin/inventory-views
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/inventory-views"]
