FROM alpine:3.21 AS chef
LABEL maintainer="ext-engineering@spicelabs.io"

RUN apk add --no-cache perl make curl gcc rustup musl-dev

RUN rustup-init -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install cargo-chef

FROM chef AS planner

WORKDIR /src/bigtent
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY benches ./benches
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

WORKDIR /src/bigtent
COPY --from=planner /src/bigtent/recipe.json recipe.json

# Copy source needed for cargo chef cook to compile dependencies
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY benches ./benches

RUN cargo chef cook --release --recipe-path recipe.json --bin bigtent

RUN cargo build --release --locked --bin bigtent

FROM alpine:3.21

COPY --from=builder /src/bigtent/target/release/bigtent /bigtent

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3000/healthz || exit 1

ENTRYPOINT ["/bigtent"]
