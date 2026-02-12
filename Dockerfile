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

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3000/healthz || exit 1

ENTRYPOINT ["/bigtent"]

