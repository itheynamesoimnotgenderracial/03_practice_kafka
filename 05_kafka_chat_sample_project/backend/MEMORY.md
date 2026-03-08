# Backend Service — Project Memory

## Architecture Summary

Multi-service Go backend (module `sample-chat`, Go 1.24.3) using Kafka as the backbone.

**Data flow**:
```
Client → api (REST) → chat.raw (Kafka) → chat-processor (EOS) → chat.validated (Kafka)
                                                                    ├─ aggregator → MongoDB
                                                                    └─ leaderboard → Redis → WebSocket clients
```

## Service Entry Points

| Service | Entry | Key deps |
|---|---|---|
| api | `cmd/api/main.go` | Gin, MongoDB, Kafka producer, Schema Registry |
| chat-processor | `cmd/chat-processor/main.go` | Kafka EOS (TxProducer), Schema Registry |
| aggregator | `cmd/aggregator/main.go` | Kafka consumer, MongoDB |
| leaderboard | `cmd/leaderboard/main.go` | Kafka consumer, Redis, WebSocket hub |

## Internal Reusable Packages

- `internal/kafka/processor.go` — `RunProcessor()` — generic EOS consume-transform-produce loop
- `internal/kafka/producer.go` — `TxProducer` wrapper for transactional Kafka producer
- `internal/kafka/config.go` — `NewProducerConfig()`, `NewConsumerConfig()` — standard config maps
- `internal/handler/chat_validation.go` — `ChatValidationHandler` (Handler func)
- `internal/handler/window_aggregator.go` — `WindowAggregatorHandler` (Handler func)
- `cmd/utils/util.go` — `GetEnv(key, default)` utility
- `cmd/pkg-models/models.go` — shared domain models

## Leaderboard Service Structure

Files in `cmd/leaderboard/`:
- `main.go` — startup, shutdown
- `hub.go` — WebSocket hub (connection manager)
- `websocket.go` — WebSocket handler
- `kafka_consumer.go` — Kafka consumer loop
- `redis.go` — Redis client
- `broadcaster.go` — fan-out logic
- `service.go` — business logic
- `leader.go` — leaderboard model/operations
- `models.go` — request/response models

## Deployment

- All services containerized via Dockerfiles in `docker/`
- Compose file: `deploy/docker-compose.yml`
- Kafka: 3-node KRaft cluster, 6 partitions, RF=3, EOS-configured
- 2x leaderboard instances behind nginx load balancer (port 8085)
- Schema Registry required before services start

## Known Patterns / Decisions

- All services use `utils.GetEnv()` — no `.env` files, no Viper
- Graceful shutdown: `context.Cancel` + signal channel in every service
- EOS pattern: Begin → Handle → Produce → SendOffsetsToTransaction → Commit; Abort on any error
- No unit tests observed yet — `go test ./...` is the test command
- Dockerfiles live in `docker/`, not beside each service

## Current Branch

`feat/websocket-hardening` — WebSocket-related hardening work in progress on leaderboard service.
