# ---------- Builder ----------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache \
    git \
    build-base \
    pkgconfig \
    bash \
    curl

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -tags musl -o chat-processor ./cmd/chat-processor

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache bash netcat-openbsd ca-certificates

COPY --from=builder /app/chat-processor /app/chat-processor
# Copy wait-for-kafka script
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/chat-processor"]
