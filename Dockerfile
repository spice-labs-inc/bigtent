FROM alpine:3.21 AS builder
LABEL maintainer="ext-engineering@spicelabs.io"

RUN apk add --no-cache perl make curl gcc rustup musl-dev

RUN rustup-init -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src/bigtent
ADD . .

RUN cargo build --release --locked

FROM alpine:3.21

COPY --from=builder /src/bigtent/target/release/bigtent /bigtent

ENTRYPOINT ["/bigtent"]

