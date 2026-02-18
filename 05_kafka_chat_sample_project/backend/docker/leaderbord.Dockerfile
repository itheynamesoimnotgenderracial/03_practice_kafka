# ---------- Builder ----------
FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git build-base pkgconfig

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o leaderboard ./cmd/leaderboard

# ---------- Runtime ----------
FROM alpine:3.20

WORKDIR /app
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/leaderboard /app/leaderboard

CMD ["/app/leaderboard"]
