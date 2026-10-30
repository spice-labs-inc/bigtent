FROM alpine:3.21 AS chef
LABEL maintainer="ext-engineering@spicelabs.io"

RUN apk add --no-cache perl make curl gcc rustup musl-dev

RUN rustup-init -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install cargo-chef

# ---------------------------------------------------------------- planner
#
# The planner exists to produce `recipe.json` — the workspace's dependency
# graph — and nothing else. Its layer must therefore depend on the MANIFESTS
# ALONE: copying a source file in here (the whole `engine/` tree, say) would
# invalidate the recipe on every source edit, and the dependency cache would be
# rebuilt for a change that cannot affect dependencies.
FROM chef AS planner

WORKDIR /src/bigtent
# every manifest in the workspace: the root's, and each member's. Cargo cannot
# enumerate a workspace without the members' manifests, but it needs nothing
# else to answer "what are the dependencies".
COPY Cargo.toml Cargo.lock ./
COPY engine/sansho/Cargo.toml ./engine/sansho/Cargo.toml
COPY engine/sansho-derive/Cargo.toml ./engine/sansho-derive/Cargo.toml
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------- builder
#
# `cook` compiles the DEPENDENCIES from the recipe. It must not see the real
# sources, or every source edit would invalidate the compiled dependency layer;
# it needs only the manifests plus a placeholder file for each target cargo
# resolves. The real sources are copied afterwards, immediately before the
# build that uses them.
FROM chef AS builder

WORKDIR /src/bigtent
COPY --from=planner /src/bigtent/recipe.json recipe.json

COPY Cargo.toml Cargo.lock ./
COPY engine/sansho/Cargo.toml ./engine/sansho/Cargo.toml
COPY engine/sansho-derive/Cargo.toml ./engine/sansho-derive/Cargo.toml
# placeholders so cargo can resolve every target it was asked to cook; their
# content is never compiled into the result
RUN mkdir -p src engine/sansho/src engine/sansho-derive/src \
    && printf 'fn main() {}\n' > src/main.rs \
    && printf '' > src/lib.rs \
    && printf '' > engine/sansho/src/lib.rs \
    && printf '' > engine/sansho-derive/src/lib.rs

RUN cargo chef cook --release --recipe-path recipe.json --bin bigtent

# THE REAL SOURCES, after the dependency layer is compiled and cached
COPY build.rs ./
COPY src ./src
COPY benches ./benches
COPY engine ./engine

RUN cargo build --release --locked --bin bigtent

FROM alpine:3.21

COPY --from=builder /src/bigtent/target/release/bigtent /bigtent

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3000/healthz || exit 1

ENTRYPOINT ["/bigtent"]
