# Chapter 18 — Exactly-Once Failure Simulation Tests

Integration tests that verify exactly-once semantics (EOS) across the chat pipeline
by simulating real production failures against the running Docker Compose cluster.

## Prerequisites

- Docker Compose cluster running (`docker compose up -d` from `deploy/`)
- All services healthy: kafka1, kafka2, kafka3, mongo, redis, processor, timeline-processor, service-aggregator
- Go 1.24+ installed locally

## Test Overview

### Pipeline Tests (require running Docker containers)

| Test | What it simulates | What it verifies |
|---|---|---|
| `TestBrokerCrashMidTransaction` | Kills kafka2 mid-processing, sends messages during outage, restarts | No duplicates, no gaps after broker recovery |
| `TestConsumerCrashAndRebalance` | Restarts timeline-processor mid-processing | Consumer resumes from correct offset, no duplicates |
| `TestNetworkPartition` | Disconnects kafka3 from Docker network | Pipeline works with 2/3 brokers, recovers after reconnect |
| `TestIdempotentReplay` | Resets aggregator consumer group offset, replays all messages | MongoDB $setOnInsert prevents duplicates on replay |

### Embedded Handler Tests (in-process, faster)

| Test | What it tests |
|---|---|
| `TestValidationHandler_ValidMessage` | Valid messages route to chat.validated |
| `TestValidationHandler_InvalidMessage_EmptyContent` | Invalid messages route to chat.raw.dlt |
| `TestValidationHandler_Sanitization` | "badword" → "***" sanitization works |
| `TestTimelineHandler_SequenceAssignment` | Sequences are contiguous (1, 2, 3...) with no gaps |
| `TestIdempotentMongoUpsert` | $setOnInsert prevents duplicates and doesn't overwrite |

## Running Tests

```bash
# From the backend directory:
cd /path/to/05_kafka_chat_sample_project/backend

# Run ALL integration tests (requires Docker cluster running)
go test ./tests/integration/ -v -count=1 -timeout 10m

# Run a specific test
go test ./tests/integration/ -v -count=1 -run TestBrokerCrashMidTransaction -timeout 5m
go test ./tests/integration/ -v -count=1 -run TestConsumerCrashAndRebalance -timeout 5m
go test ./tests/integration/ -v -count=1 -run TestNetworkPartition -timeout 5m
go test ./tests/integration/ -v -count=1 -run TestIdempotentReplay -timeout 5m

# Run only embedded handler tests (faster, no Docker container manipulation)
go test ./tests/integration/ -v -count=1 -run "TestValidation|TestTimeline|TestIdempotentMongo" -timeout 2m

# Skip integration tests during normal development
go test ./tests/integration/ -short
```

## Important Notes

### Docker Network Name

The network partition test assumes your Docker network is named `deploy_kafka-net`.
This is derived from your docker-compose directory name (`deploy`) + the network name (`kafka-net`).

To verify your network name:
```bash
docker network ls | grep kafka
```

If it differs, update the `network` variable in `TestNetworkPartition`.

### Container Names

Tests reference these container names (from your docker-compose.yml):
- `kafka1`, `kafka2`, `kafka3` — Kafka brokers
- `timeline-processor` — timeline processor service
- `service-aggregator` — aggregator service

### Test Isolation

Each test uses a **unique room ID** (e.g., `eos-broker-crash-a1b2c3d4`) so tests
don't interfere with each other or with your development data. Test data is
cleaned up automatically via `defer infra.CleanupTestRoom()`.

### Timeouts

Pipeline tests involve Docker container restarts and Kafka rebalances, which
can take 15-30 seconds. Use `-timeout 5m` or higher for individual tests
and `-timeout 10m` when running all tests together.

### Running Order

Tests are independent and can run in any order. However, they do manipulate
Docker containers, so avoid running broker crash / network partition tests
concurrently. Use `-count=1` to disable test caching.

## What Exactly-Once Means Here

The pipeline provides exactly-once semantics through three layers:

1. **Kafka Transactions (EOS)** — chat-processor and timeline-processor use
   transactional producers with `sendOffsetsToTransaction`. This means
   "consume + produce + commit offset" is atomic.

2. **Idempotent Producers** — the API service uses `enable.idempotence=true`
   with `acks=all`, preventing duplicate produces at the Kafka level.

3. **Idempotent MongoDB Upserts** — the aggregator uses `$setOnInsert` keyed
   on `message_id` when persisting to `chat_messages`. Replaying the same
   Kafka message produces the same MongoDB write, which is a no-op.

These tests verify all three layers under real failure conditions.