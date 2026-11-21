# Stage 1: Compile the Rust application
FROM rust:1.100.0-bookworm AS builder

WORKDIR /app

COPY cr_instance_dummy/Cargo.toml cr_instance_dummy/Cargo.lock ./
COPY cr_instance_dummy/src ./src
RUN cargo build --release --locked

# Stage 2: Run the compiled binary
FROM debian:bookworm-slim

COPY --from=builder /app/target/release/cr_instance_dummy /usr/local/bin/cr_instance_dummy

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/cr_instance_dummy"]
