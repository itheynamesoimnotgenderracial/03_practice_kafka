FROM golang:1.24-alpine as builder

WORKDIR /app

RUN apk add --no-cache git bash curl

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o processor ./cmd/processor

# ---------- Runtime stage ----------
FROM alpine:3.20

WORKDIR /app

# Install runtime deps
RUN apk add --no-cache bash netcat-openbsd ca-certificates

COPY --from=builder /app/processor /app/processor

# Copy wait-for-kafka script
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/processor"]