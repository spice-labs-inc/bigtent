# syntax=docker/dockerfile:1
#
# Multi-target BigTent build.
#
# The toolchain stages (chef, planner, deps, deps-test, builder, tester) are
# pinned to $BUILDPLATFORM so cargo-chef and the Rust toolchain are installed
# *once* on the build host. The linux/arm64 image is produced by
# cross-compiling to aarch64-unknown-linux-musl with the musl.cc toolchain
# (no QEMU, no emulated `cargo install`).
#
# Dependency cooking lives in its own stage (`deps`/`deps-test`), so BuildKit
# caches it independently of source changes: editing src/*.rs re-runs only the
# final `cargo build`, not the dependency compile.

# ---------------------------------------------------------------------------
# chef: shared base image — Rust toolchain + cargo-chef + arm64 musl cross CC
# ---------------------------------------------------------------------------
# Pinned by digest (manifest-list covers amd64 + arm64).
FROM --platform=$BUILDPLATFORM alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d AS chef
LABEL maintainer="ext-engineering@spicelabs.io"

# Pin cargo-chef and the Rust toolchain so the base image is reproducible.
# Rust 1.95.0 matches the project's rust-version (rust-toolchain.toml).
ARG CARGO_CHEF_VERSION=0.1.77
ARG RUST_VERSION=1.95.0

RUN apk add --no-cache perl make gcc rustup musl-dev ca-certificates \
 && rustup-init -y --profile minimal --default-toolchain "${RUST_VERSION}"

ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install --locked --version "${CARGO_CHEF_VERSION}" cargo-chef

# aarch64-unknown-linux-musl target for cross-compiling the arm64 image.
RUN rustup target add aarch64-unknown-linux-musl

# aarch64 musl cross-toolchain (aarch64-linux-musl-gcc / -ar), used to build the
# only target-arch C dependency (zstd-sys, via tower-http compression). Pulled
# from the spice-labs GHCR mirror so CI doesn't depend on musl.cc availability.
COPY --from=ghcr.io/spice-labs-inc/musl-cc-mirror:aarch64-linux-musl-v1 \
     /aarch64-linux-musl-cross /opt/aarch64-linux-musl-cross
ENV PATH="/opt/aarch64-linux-musl-cross/bin:${PATH}"

# ---------------------------------------------------------------------------
# planner: emit cargo-chef recipes
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM chef AS planner
WORKDIR /workspace
COPY Cargo.toml Cargo.lock build.rs rust-toolchain.toml ./
COPY src ./src
COPY benches ./benches
# Bin-scoped recipe: cooking it with `--bin bigtent` caches the full dependency
# tree (tokio, axum, tonic, opentelemetry, ...) while skipping the
# data_generator target, which is declared as both [[bin]] and [[bench]] on
# the same path and trips cargo-chef's stub generator when cooked as a bench.
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# deps: cook release dependencies (cross-compiled when TARGETARCH=arm64)
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM chef AS deps
ARG TARGETARCH
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/target
COPY --from=planner /workspace/recipe.json recipe.json
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
      AR_aarch64_unknown_linux_musl=aarch64-linux-musl-ar \
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc \
      cargo chef cook --release --bin bigtent --recipe-path recipe.json \
        --target aarch64-unknown-linux-musl; \
    else \
      cargo chef cook --release --bin bigtent --recipe-path recipe.json; \
    fi

# ---------------------------------------------------------------------------
# deps-test: cook dependencies for the full test suite (native build host)
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM chef AS deps-test
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/target
COPY --from=planner /workspace/recipe.json recipe.json
# Cook bigtent's dependency tree in debug mode (cargo test runs in debug).
# --bin bigtent scopes the cook to bigtent's deps; dev-deps (tempfile,
# criterion, proptest) are small and compile fresh in the tester.
RUN cargo chef cook --bin bigtent --recipe-path recipe.json

# ---------------------------------------------------------------------------
# builder: build the bigtent release binary (cross-compiled for arm64)
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM deps AS builder
ARG TARGETARCH
ARG GIT_SHA=unknown
ENV VERGEN_GIT_SHA=${GIT_SHA}
WORKDIR /workspace
COPY Cargo.toml Cargo.lock build.rs rust-toolchain.toml ./
COPY src ./src
COPY benches ./benches
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
      AR_aarch64_unknown_linux_musl=aarch64-linux-musl-ar \
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc \
      cargo build --locked --release --bin bigtent \
        --target aarch64-unknown-linux-musl \
        && cp /target/aarch64-unknown-linux-musl/release/bigtent /tmp/bigtent; \
    else \
      cargo build --locked --release --bin bigtent \
        && cp /target/release/bigtent /tmp/bigtent; \
    fi

# ---------------------------------------------------------------------------
# tester: build and run the library test suite
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM deps-test AS tester
WORKDIR /workspace
COPY Cargo.toml Cargo.lock build.rs rust-toolchain.toml ./
COPY src ./src
COPY benches ./benches
COPY test_data ./test_data
RUN cargo test --locked --lib

# ---------------------------------------------------------------------------
# bigtent: runtime image
# ---------------------------------------------------------------------------
# Pinned by digest (manifest-list covers amd64 + arm64).
FROM alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d AS bigtent
COPY --from=builder /tmp/bigtent /bigtent

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3000/healthz || exit 1

ENTRYPOINT ["/bigtent"]
