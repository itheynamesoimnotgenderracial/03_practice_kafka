# Backend Service — Project Memory

## Data Flow (Full Pipeline)

```
Client → POST /api/messages
  → [api] produces ChatRawEvent → chat.raw (Avro, key=room_id)
  → [chat-processor] validates + sanitizes → chat.validated OR chat.raw.dlt (EOS)
  → [timeline-processor] assigns sequence → chat.timeline (EOS)
  → [aggregator] window aggregation → room_metrics (MongoDB) + chat.leaderboard (Avro)
  → [leaderboard] Redis ZSet update → leaderboard_updates (pub/sub) → WebSocket clients
```

## Service Entry Points

| Service | Entry | Consumer Group | Producer TxID |
|---|---|---|---|
| api | `cmd/api/main.go` | — | none (idempotent) |
| chat-processor | `cmd/chat-processor/main.go` | `chat-processor-group` | `chat-processor-1` |
| timeline-processor | `cmd/timeline-processor/main.go` | `timeline-processor-group` | (see main.go) |
| aggregator | `cmd/aggregator/main.go` | `aggregator-group` | none (no EOS) |
| leaderboard | `cmd/leaderboard/main.go` | `leaderboard-group` | none |

## Internal Packages — Key Exports

### `internal/kafka/`
- `RunProcessor(ctx, consumer, txProducer, handler)` — generic EOS consume-transform-produce loop
- `TxProducer` — `Begin()`, `Produce(topic, key, value)`, `Commit(ctx)`, `Abort(ctx)`
- `NewProducerConfig(brokers, txID)` — transactional producer config
- `NewConsumerConfig(brokers, groupID)` — auto-commit=false, isolation=read_committed
- `ChatLeaderboardEvent` struct (shared model in `model.go`)
- `consumer.go` is empty — placeholder

### `internal/handler/`
- `ChatValidationHandler` (in `chat_validation.go`) — implements `Handler` func signature
  - Deserializes `ChatRawEvent`, validates fields, sanitizes "badword" → "***"
  - Returns `ChatValidatedEvent` to `chat.validated` OR `ChatDLTEvent` (JSON) to `chat.raw.dlt`
- `NewTimelineHandler` (in `timeline_handler.go`) — implements `Handler` func signature
  - Deserializes `ChatValidatedEvent`, calls `seqStore.NextSequence(roomID)`
  - Returns `ChatTimelineEvent` (with sequence) to `chat.timeline`
- `ComputeHourlyWindow(timestamp int64) time.Time` — truncates to hour boundary
- `ComputeDailyWindow(timestamp int64) time.Time` — truncates to day boundary (UTC)
- `toTime(timestamp int64) time.Time` — handles both unix seconds and milliseconds

### `internal/mongo/`
- `SequenceStore` → `NextSequence(ctx, roomID) (int64, error)`
  - Collection: `room_sequences`, unique index on `room_id`
  - `FindOneAndUpdate($inc: {sequence: 1}, upsert=true)` — atomic, starts at 1

## API Service Details (`cmd/api/`)

**Routes**: `GET /api/messages`, `POST /api/messages`

- `GET /api/messages?roomId=X&limit=30&before=<timestamp>` — queries `chat_messages`, sorted by sequence DESC, paginated
- `POST /api/messages` — reads `user_id` from request header, creates UUID message ID, serializes to Avro, produces to `chat.raw`
  - Uses delivery channel (chan kafka.Event) to await ACK before responding 202

**Repository** (`cmd/api/repository/message_repository.go`):
- Collection: `chat_messages`
- Pagination: filter by `room_id` + `sequence < before`, limit N, sort by sequence DESC

## Chat Processor Service (`cmd/chat-processor/`)

- Single file: `main.go`
- Wires up: Avro deserializer + serializer → `ChatValidationHandler` → `RunProcessor`
- Consumer group: `chat-processor-group`, topic: `chat.raw`
- Validation rules: message_id, room_id, user_id, content (non-empty, ≤4096 chars), timestamp > 0
- On invalid: produces `ChatDLTEvent` as JSON to `chat.raw.dlt` (still transactional)

## Timeline Processor Service (`cmd/timeline-processor/`)

- Single file: `main.go`
- Wires up: Avro deserializer + serializer + `SequenceStore` → `NewTimelineHandler` → `RunProcessor`
- Consumer group: `timeline-processor-group`, topic: `chat.validated`
- Ensures global ordering of messages per room via MongoDB-based sequence

## Aggregator Service (`cmd/aggregator/`)

- `main.go` + `models.go`
- No EOS — uses simple consumer loop (not `RunProcessor`)
- Consumer group: `aggregator-group`, topic: `chat.timeline`
- Per message: computes hourly AND daily windows, upserts into MongoDB `room_metrics`
  - MongoDB update: `$inc total_messages`, `$addToSet active_users`, `$set finalized=true, window_start, window_type`
  - Reads back updated doc to get `TotalMessages` + `ActiveUsersCount`
- Emits two `ChatLeaderboardEvent`s per message (one hourly, one daily) to `chat.leaderboard`

**Models** (`cmd/aggregator/models.go`):
- `ChatTimelineEvent` — consumed input
- `ChatLeaderboardEvent` — produced output
- `RoomMetrics` — MongoDB document shape

## Leaderboard Service (`cmd/leaderboard/`)

**main.go** — runs two goroutines concurrently:
1. `StartKafkaConsumer(ctx, redisStore)` — Kafka consumer loop
2. `StartWebsocketServer(ctx, redisStore)` — WebSocket HTTP server + Redis subscriber

Graceful shutdown: 15s timeout after context cancel.

**kafka_consumer.go**:
- Subscribes to `chat.leaderboard`, consumer group `leaderboard-group`
- Per message: `redis.UpdateScore(roomID, totalMessages)` → `redis.GetTopN(10)` → JSON marshal → `redis.PublishLeaderboard(payload)`

**redis.go** — `RedisClientStore`:
- `UpdateScore(ctx, roomID, score)` — `ZIncrBy("leaderboard", score, roomID)`
- `GetTopN(ctx, n)` — `ZRevRangeWithScores("leaderboard", 0, n-1)`
- `PublishLeaderboard(ctx, payload)` — `Publish("leaderboard_updates", payload)`
- Generates `instanceID` via `crypto/rand` (used for future leader election)

**hub.go** — `Hub`:
- Fields: `clients map[*Client]bool`, `register/unregister/broadcast` channels
- Client send buffer: 256 messages; slow clients are dropped
- Timeouts: write=10s, pong=90s, ping period=30s

**websocket.go** — `StartWebsocketServer`:
- HTTP on `:8084`, endpoint `/ws`, CORS open (`CheckOrigin` returns true)
- On connect: creates `Client`, registers with `Hub`, starts `readPump` + `writePump`
- Goroutine subscribes to Redis `leaderboard_updates` and calls `hub.Broadcast(msg)`
- Graceful: broadcasts shutdown message, waits 2s, then shuts down HTTP server

**Not yet integrated into main.go**:
- `broadcaster.go` — `StartBroadcaster`: publishes `"refresh"` every 5s (unused)
- `leader.go` — `LeaderManagerStore`: Redis distributed lock for leader election (unused)
- `service.go` — MongoDB-backed service (all commented out)

## MongoDB Collections

| Collection | Index | Purpose |
|---|---|---|
| `chat_messages` | `(room_id, sequence)` | Message history for API reads |
| `room_sequences` | unique `room_id` | Per-room atomic sequence counters |
| `room_metrics` | `(room_id, window_start, window_type)` | Aggregated window stats |

## Redis Keys

| Key | Type | Operations |
|---|---|---|
| `leaderboard` | Sorted Set | `ZIncrBy`, `ZRevRangeWithScores` |
| `leaderboard_updates` | Pub/Sub | `Publish` / `Subscribe` |
| `leaderboard_lock` | String TTL 10s | `SetNX` for leader election (not in use) |

## Shared Models (`cmd/pkg-models/models.go`)

- `ChatMessage` — full message with Avro + JSON + MongoDB tags, includes `sequence`
- `ChatRawEvent` — raw input event (no sequence)

## Known Incomplete / Future Work

- `internal/kafka/consumer.go` — empty file, placeholder
- `cmd/leaderboard/service.go` — all code commented out (MongoDB-backed fallback)
- `cmd/leaderboard/leader.go` + `broadcaster.go` — leader election infrastructure not wired into `main.go`
- Aggregator lacks EOS — relies on MongoDB upsert idempotency instead
- No unit tests observed; `go test ./...` is the test command

## Current Branch

`feat/websocket-hardening` — WebSocket hardening work on the leaderboard service.
