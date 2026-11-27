FROM rust:1.100.0-bookworm AS builder

WORKDIR /app

COPY trust_ingestor/Cargo.toml trust_ingestor/Cargo.lock ./
COPY trust_ingestor/src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/trust_ingestor /usr/local/bin/trust_ingestor

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/trust_ingestor"]
