🪜 Learning Roadmap (Follow in Order)
Phase 1 — Event Ingestion (Foundation)

Gin HTTP API

Kafka producer

Topic design

*========================== ==========================*

Phase 2 — Stream Processing

Kafka consumer group

Filtering & transformation

Error handling & logging

*========================== ==========================*

Phase 3 — Persistence

MongoDB integration

Idempotent writes

Persistence validation

*========================== ==========================*

Phase 4 — Analytics (Later)

Aggregation consumers

Dashboard APIs

Real-time updates



================================== Stream processing ==================================

🔨 PHASE 2 IMPLEMENTATION PLAN (Step-by-step)
STEP 1 — Topic Design (next)

We define:

input topic

output topics

partition keys

retention

⬅️ This is our very next task

STEP 2 — Aggregator Service Skeleton

Kafka consumer

in-memory map

ticker-based flush

STEP 3 — Windowed Aggregation

1-minute tumbling window

per-user aggregation

emit metrics event

STEP 4 — Output Topics

produce aggregated metrics

partition by user_id

STEP 5 — Optional Persistence

MongoDB snapshot every N minutes

recovery on restart

⚠️ Important: What we are NOT doing (yet)

Kafka Streams API

Flink / Spark

Exactly-once semantics

We’re building mental models first, not magic abstractions.