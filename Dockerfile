FROM rust:1.51.0 as builder
WORKDIR app
RUN mkdir src && echo "fn main() {}" >> src/main.rs
COPY Cargo.toml .
RUN cargo build --release
COPY . .
RUN cargo build --release

FROM gcr.io/distroless/cc
COPY --from=builder /app/target/release/spreading-operator /
CMD ["./spreading-operator"]