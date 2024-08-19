FROM messense/rust-musl-cross:x86_64-musl AS build

LABEL MAINTAINER="engineering@spicelabs.io"
ARG APP_NAME=bigtent

WORKDIR /opt/docker/bigtent

COPY ./ ./

RUN cargo build --locked --release

ENTRYPOINT ["/opt/docker/bigtent/target/x86_64-unknown-linux-musl/release/bigtent"]
