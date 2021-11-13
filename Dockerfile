FROM rust:1.56-buster as build

RUN cargo new --bin app
WORKDIR /app

# Build and cache dependencies
COPY ./Cargo.toml ./Cargo.lock ./
RUN cargo build --release
RUN cargo clean --release --package near-epoch-indexer && rm -r src/

COPY . .
RUN cargo build --release

FROM debian:buster
RUN apt-get update -qq && apt-get install -y \
    libpq-dev
COPY --from=build /app/target/release/near-epoch-indexer /app/near-epoch-indexer

ENTRYPOINT ["/app/near-epoch-indexer"]