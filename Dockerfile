# ---- Build Stage ----
# Build context is the parent dir holding gateway, drift-rs, drift-ffi-sys and
# protocol-v2 as siblings (see Cargo.toml path deps and drift-rs build.rs).
FROM rust:1.85 AS builder

RUN apt-get update && apt-get install -y libgcc1 jq
WORKDIR /app
RUN rustup component add rustfmt

COPY protocol-v2 ./protocol-v2
COPY drift-ffi-sys ./drift-ffi-sys
COPY drift-rs ./drift-rs
COPY gateway ./gateway

WORKDIR /app/gateway
# build libdrift_ffi_sys from source via drift-rs build.rs (../drift-ffi-sys sibling)
RUN cargo build --release

RUN cp /lib/x86_64-linux-gnu/libgcc_s.so.1 /app/gateway/target/release/

# ---- Runtime Stage ----
FROM debian:12
COPY --from=builder /app/gateway/target/release/libgcc_s.so.1 /lib/
COPY --from=builder /app/drift-ffi-sys/target/release/libdrift_ffi_sys.so /lib/
COPY --from=builder /app/gateway/target/release/drift-gateway /bin/drift-gateway
RUN apt-get update && apt-get install -y curl && rm -rf /var/cache/apt/archives /var/lib/apt/lists/*
ENTRYPOINT ["/bin/drift-gateway"]
