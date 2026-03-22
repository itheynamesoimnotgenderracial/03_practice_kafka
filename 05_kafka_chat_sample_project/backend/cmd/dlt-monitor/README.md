# Chapter 19 — Dead Letter Topics & Retry Patterns

Complete DLT strategy for the chat pipeline: retry topics with exponential backoff,
poison pill detection with panic recovery, DLT monitoring, and a REST API for
inspecting and replaying dead-lettered messages.

## Architecture

```
                        ┌──────────────────┐
                        │   chat.raw       │
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │  chat-processor   │◄─── SafeHandler (panic recovery)
                        │  (validation)     │
                        └──┬──────────┬─────┘
                           │          │
              valid ───────┘          └──── invalid/panic
                           │                    │
                  ┌────────▼───────┐   ┌───────▼────────────┐
                  │ chat.validated  │   │ Classify error:    │
                  └────────────────┘   │ • validation → DLT │
                                       │ • panic → DLT      │
                                       │ • transient → retry │
                                       └──┬──────────┬──────┘
                                          │          │
                               ┌──────────▼──┐  ┌───▼──────────┐
                               │ chat.raw.dlt │  │chat.raw.retry│
                               └──────┬──────┘  └──────┬───────┘
                                      │                 │
                            ┌─────────▼───────┐  ┌─────▼─────────────┐
                            │  dlt-monitor    │  │  retry-consumer   │
                            │  • structured   │  │  • backoff delay  │
                            │    logging      │  │  • max 3 retries  │
                            │  • MongoDB      │  │  • re-route to    │
                            │    persistence  │  │    chat.raw or DLT│
                            └─────────────────┘  └───────────────────┘
                                      │
                            ┌─────────▼──────────┐
                            │  API: /api/dlt     │
                            │  • List events     │
                            │  • View details    │
                            │  • Replay to raw   │
                            │  • Stats dashboard │
                            └────────────────────┘
```

## New Files

| File | Purpose |
|---|---|
| `internal/dlt/dlt.go` | DLT package: event types, header helpers, routing logic, retry constants |
| `internal/handler/safe_handler.go` | Panic recovery wrapper for handlers (SafeHandler, SafeHandlerWithRetry) |
| `cmd/dlt-monitor/main.go` | DLT consumer service: logs events, persists to MongoDB `dlt_events` |
| `cmd/retry-consumer/main.go` | Retry consumer: exponential backoff, re-routes to chat.raw or DLT |
| `cmd/api/dltapi/dlt_handler.go` | REST API: GET /api/dlt, GET /api/dlt/:messageId, POST /api/dlt/:messageId/replay, GET /api/dlt/stats |

## Setup

### 1. Create the retry topic

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic chat.raw.retry \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --bootstrap-server kafka1:29092
```

### 2. Add Docker Compose services

Add these to your `docker-compose.yml`:

```yaml
  dlt-monitor:
    build:
      context: ../backend
      dockerfile: cmd/dlt-monitor/Dockerfile
    container_name: dlt-monitor
    environment:
      KAFKA_BROKERS: kafka1:29092
      MONGO_URI: mongodb://mongo:27017
    depends_on:
      - kafka1
      - mongo
    networks:
      - kafka-net
    restart: unless-stopped

  retry-consumer:
    build:
      context: ../backend
      dockerfile: cmd/retry-consumer/Dockerfile
    container_name: retry-consumer
    environment:
      KAFKA_BROKERS: kafka1:29092
    depends_on:
      - kafka1
    networks:
      - kafka-net
    restart: unless-stopped
```

### 3. Wire DLT API into the API service

In your `cmd/api/main.go`, add:

```go
import "sample-chat/cmd/api/dltapi"

// After creating the Gin router and Kafka producer:
dltHandler := dltapi.NewDLTAPIHandler(db, producer)
dltHandler.RegisterRoutes(router.Group("/api"))
```

### 4. Wrap handlers with panic recovery

In your `cmd/chat-processor/main.go`, wrap the validation handler:

```go
import "sample-chat/internal/handler"

// Before:
validationHandler := handler.ChatValidationHandler(deserializer, serializer)

// After:
validationHandler := handler.SafeHandler(
    handler.ChatValidationHandler(deserializer, serializer),
)
```

### 5. Build and deploy

```bash
docker compose up -d --build dlt-monitor retry-consumer api
```

## Error Classification

| Error Type | Destination | Retryable? | Examples |
|---|---|---|---|
| `validation` | DLT (permanent) | No | Empty content, missing room_id, content too long |
| `deserialization` | DLT (permanent) | No | Corrupt Avro, schema mismatch |
| `panic` | DLT (permanent) | No | Nil pointer dereference, index out of range |
| `transient` | Retry topic | Yes (max 3) | MongoDB timeout, Schema Registry unavailable |

## Retry Behavior

- **Exponential backoff**: 5s → 10s → 20s (capped at 60s)
- **Max retries**: 3 attempts before routing to DLT permanently
- **Retry headers**: `x-retry-count`, `x-original-topic`, `x-failure-reason`, `x-failed-at`, `x-error-type`
- **Re-routing**: Retry consumer sends the message back to `chat.raw` for full reprocessing

## DLT API Endpoints

### List DLT events
```bash
# All events (newest first)
curl http://localhost:8083/api/dlt

# Filter by error type
curl "http://localhost:8083/api/dlt?error_type=validation&limit=10"

# Only unreplayed events
curl "http://localhost:8083/api/dlt?replayed=false"
```

### View a specific DLT event
```bash
curl http://localhost:8083/api/dlt/{messageId}
```

### Replay a DLT message back to chat.raw
```bash
curl -X POST http://localhost:8083/api/dlt/{messageId}/replay
```

### Get DLT statistics
```bash
curl http://localhost:8083/api/dlt/stats
# Returns: { total, unreplayed, replayed, by_error_type: [...] }
```

## MongoDB Collection: dlt_events

```json
{
  "message_id": "abc-123",
  "original_topic": "chat.raw",
  "error_type": "validation",
  "failure_reason": "content is empty after trimming",
  "retry_count": 0,
  "raw_payload": "<original Avro bytes>",
  "key": "room-1",
  "partition": 3,
  "offset": 42,
  "failed_at": 1711065600000,
  "created_at": 1711065600100,
  "replayed": false
}
```

Indexes: `message_id`, `error_type`, `created_at` (desc), `replayed`.

## Testing the DLT Pipeline

```bash
# Send a message with empty content (triggers validation → DLT)
curl -X POST http://localhost:8083/api/messages \
  -H "Content-Type: application/json" \
  -H "user_id: test-user" \
  -d '{"room_id": "test-room", "content": "   "}'

# Check DLT monitor logs
docker logs dlt-monitor --tail 20

# View DLT events via API
curl http://localhost:8083/api/dlt

# Replay a failed message
curl -X POST http://localhost:8083/api/dlt/{messageId}/replay
```