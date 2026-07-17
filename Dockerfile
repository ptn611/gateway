FROM rust:1.96 AS builder

RUN apt-get update && apt-get install -y libgcc1
WORKDIR /build
RUN rustup component add rustfmt

COPY  . .
RUN cargo build --release

RUN cp /lib/x86_64-linux-gnu/libgcc_s.so.1 /build/target/release/

FROM debian:12
COPY --from=builder /build/target/release/libgcc_s.so.1 /lib/
COPY --from=builder /build/target/release/velocity-gateway /bin/velocity-gateway
RUN apt-get update && apt-get install -y curl && rm -rf /var/cache/apt/archives /var/lib/apt/lists/*
ENTRYPOINT ["/bin/velocity-gateway"]
