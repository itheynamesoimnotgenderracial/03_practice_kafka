FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build aggregator binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o aggregator ./cmd/aggregator

# ---------- Runtime stage ----------
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/aggregator /app/aggregator

USER nonroot:nonroot

ENTRYPOINT ["/app/aggregator"]
