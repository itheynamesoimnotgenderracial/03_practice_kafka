# ---------- Builder ----------
FROM golang:1.24-alpine AS builder

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

RUN go build -tags musl -o timeline-processor ./cmd/timeline-processor

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache bash netcat-openbsd ca-certificates

COPY --from=builder /app/timeline-processor /app/timeline-processor
# Copy wait-for-kafka script
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/timeline-processor"]
