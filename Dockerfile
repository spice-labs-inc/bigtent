FROM messense/rust-musl-cross:x86_64-musl AS build

LABEL MAINTAINER="ext-engineering@spicelabs.io"


# Create a new empty shell project
RUN USER=root cargo new --bin bigtent
WORKDIR /bigtent

# copy over manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# copy source tree
COPY ./src ./src

# build for release
RUN cargo build --release

# final base image
FROM messense/rust-musl-cross:x86_64-musl

# copy the build artifact from the build stage
COPY --from=build /bigtent/target/x86_64-unknown-linux-musl/release/bigtent  .

ENTRYPOINT ["./bigtent"]
