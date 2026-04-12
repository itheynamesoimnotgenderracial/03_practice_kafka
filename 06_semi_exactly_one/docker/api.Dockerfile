# ---------- Builder ----------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache \
    git \
    build-base \
    pkgconfig \
    librdkafka-dev \
    openssl-dev \
    zlib-dev \
    zstd-dev \
    lz4-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build \
    -tags musl \
    -ldflags="-w -s" \
    -v \
    -o /app/api ./cmd/api_service \
    && echo "Build succeeded" \
    && ls -la /app/api \
    && file /app/api

# Verify the binary is a real ELF executable — fails the build if it's not
RUN file /app/api | grep -q "ELF" || (echo "ERROR: /app/api is not a valid ELF binary" && exit 1)
RUN chmod +x /app/api

# ---------- Final image ----------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache \
    ca-certificates \
    librdkafka \
    netcat-openbsd \
    file

COPY --from=builder /app/api /app/api
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /app/api /wait-for-kafka.sh

# Verify again in the final image
RUN file /app/api

EXPOSE 8083

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/api"]