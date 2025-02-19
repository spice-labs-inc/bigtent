FROM alpine:3.21 AS builder
LABEL MAINTAINER="ext-engineering@spicelabs.io"

RUN apk add perl make curl gcc rustup musl-dev

RUN rustup-init -y

ADD . /src/bigtent
WORKDIR /src/bigtent

ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo build --release

FROM alpine:3.21
COPY --from=builder /src/bigtent/target/release/bigtent /bigtent
CMD ["/bigtent"]
