# ---------- Builder ----------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache \
    git \
    build-base \
    pkgconfig

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -tags musl -o aggregator ./cmd/aggregator

# ---------- Runtime ----------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/aggregator /app/aggregator
# Copy wait-for-kafka script
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/aggregator"]
