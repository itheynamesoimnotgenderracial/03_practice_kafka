# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Go service demonstrating **semi-exactly-once semantics** with Kafka. The goal is to guarantee that each customer message is processed exactly once end-to-end, even across crashes, retries, and duplicate Kafka deliveries. It uses an outbox pattern with a PostgreSQL checkpoint table to achieve idempotency without a full transactional producer.

## Commands

### Run locally (Docker)
```bash
cd deploy
docker compose up --build
```

### Build binary (requires librdkafka — use Docker for CGO builds)
```bash
CGO_ENABLED=1 go build -tags musl -o api ./cmd/api_service
```

### Create Kafka topics (run after `docker compose up`)
```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic simple.eos.validator --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --bootstrap-server kafka1:29092
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic simple.eos.consumer --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --bootstrap-server kafka1:29092
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic simple.eos.dlq --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --bootstrap-server kafka1:29092
```

### Run a single integration test
```bash
go test ./tests/integration/ -v -count=1 -run TestConsumerCrashAndRebalance -timeout 5m
```

## Architecture

### Message flow

```
POST /api/customer
    → Gin handler (internal/api/handler.go)
    → Publishes to simple.eos.validator (keyed by customer_id)
    → ValidatorConsumer (internal/consumer/validator.go)
        → Validates fields, forwards to simple.eos.consumer
        → Invalid → simple.eos.dlq
    → CustomerConsumer (internal/consumer/consumer.go)
        → Idempotency + outbox checkpointing + external API call + DB save
```

The single binary (`cmd/api_service/main.go`) runs both the HTTP API and both consumers concurrently.

### Idempotency / outbox pattern (`internal/consumer/consumer.go`)

The `processWithIdempotency` function is the core of the exactly-once guarantee. The outbox table (`internal/db/db.go`) tracks each message through these states:

| Status | Meaning |
|---|---|
| `PENDING` | Outbox row created; external API not yet called |
| `EXT_API_DONE` | External API succeeded; DB save not yet completed |
| `COMPLETED` | Customer upserted and outbox closed atomically |
| `FAILED` | Exhausted retries; sent to DLQ |

On redelivery the consumer checks the outbox and resumes from the correct step, preventing duplicate external API calls. Kafka offsets are committed **last**, only after the DB transaction commits.

#### EXT_API_DONE crash loop protection

If the process keeps crashing during `saveToDatabase` (e.g. persistent DB outage or OOM kill), the outbox status stays `EXT_API_DONE` and Kafka keeps redelivering the same message, causing an infinite loop. To break this, `retry_count` is incremented on every `EXT_API_DONE` resume. Once it reaches `maxRetries` (3), the consumer bails: marks the outbox `FAILED` with step `EXT_API_DONE_CRASH_LOOP`, sends to DLQ, and commits the offset — ending the loop. This is implemented via `db.Store.IncrementRetryCount`.

Idempotency key format: `{message_id}::{customer_id}::{action}` (built in `internal/consumer/helpers.go`).

### Key interfaces

- `kafka.Producer` / `kafka.Consumer` (`internal/kafka/kafka.go`) — thin wrappers around confluent-kafka-go; inject mocks in tests.
- `db.Store` (`internal/db/db.go`) — all DB access goes through this interface; `PostgresStore` is the real implementation; migrate runs inline on startup. Key methods: `GetOutboxRecord`, `CreateOutboxRecord`, `UpdateOutboxStatus`, `MarkOutboxFailed`, `IncrementRetryCount`, `SaveCustomerAndCompleteOutbox`.
- `external.CallerFunc` (`external/api.go`) — function type for the third-party API; simulates 30% failure rate in the default implementation.

### Infrastructure (deploy/docker-compose.yml)

| Service | Port | Purpose |
|---|---|---|
| kafka1 (controller+broker) | 9092 | KRaft cluster entry point |
| kafka2, kafka3 | 9094, 9095 | Additional brokers (replication factor 3) |
| kafka-ui | 8089 | Provectus Kafka UI |
| schema-registry | 8081 | Confluent Schema Registry |
| api-service | 8084 | This application |
| postgres | 5432 | Customer + outbox tables |
| pgadmin | 15432 | pgAdmin4 (test@test.com / postgres) |

Kafka is configured for EOS: `acks=all`, `enable.idempotence=true`, `min.insync.replicas=2`, `unclean.leader.election=false`, replication factor 3.

### Config (`internal/config/config.go`)

All config is via environment variables with sensible defaults for local dev:

| Env var | Default |
|---|---|
| `POSTGRES_DSN` | `postgres://postgres:postgres@localhost:5432/customerdb?sslmode=disable` |
| `KAFKA_BROKERS` | `kafka1:29092` |
| `KAFKA_GROUP_ID` | `semi-eos` |
| `KAFKA_VALIDATION_TOPIC` | `simple.eos.validator` |
| `KAFKA_CONSUMER_TOPIC` | `simple.eos.consumer` |
| `KAFKA_DLQ_TOPIC` | `simple.eos.dlq` |
| `PORT` | `8084` |
