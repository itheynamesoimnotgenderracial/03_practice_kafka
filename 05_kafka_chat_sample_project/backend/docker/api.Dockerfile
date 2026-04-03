
# FROM golang:1.24-alpine as builder

# WORKDIR /app

# RUN apk add --no-cache git

# COPY go.mod go.sum ./
# RUN go mod download

# COPY . .

# # Build binary
# RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
#     go build -o api ./cmd/api

# # ---------- Runtime stage ----------
# FROM gcr.io/distroless/base-debian12

# WORKDIR /app

# COPY --from=builder /app/api /app/api

# EXPOSE 8083

# USER nonroot:nonroot

# ENTRYPOINT [ "/app/api" ]




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

RUN go build -tags musl -o api ./cmd/api

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/api /app/api
# Copy wait-for-kafka script
COPY --from=builder /app/scripts/wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

EXPOSE 8083

ENTRYPOINT ["/wait-for-kafka.sh"]
CMD ["/app/api"]
