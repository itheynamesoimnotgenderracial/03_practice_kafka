package serde

// CustomerMessageAvsc is the Avro schema for the CustomerMessage type.
// Registered under subjects:
//   - simple.eos.validator-value
//   - simple.eos.consumer-value
//
// This must stay in sync with schemas/customer_message.avsc.
const CustomerMessageAvsc = `{
  "type": "record",
  "name": "CustomerMessage",
  "namespace": "com.simple.eos",
  "fields": [
    {"name": "message_id",  "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "name",        "type": "string"},
    {"name": "email",       "type": "string"},
    {"name": "action",      "type": {"type": "enum", "name": "Action", "symbols": ["CREATE", "UPDATE"]}}
  ]
}`

// DLQMessageAvsc is the Avro schema for the DLQMessage type.
// Registered under subject:
//   - simple.eos.dlq-value
//
// This must stay in sync with schemas/dlq_message.avsc.
const DLQMessageAvsc = `{
  "type": "record",
  "name": "DLQMessage",
  "namespace": "com.simple.eos",
  "fields": [
    {
      "name": "original_payload",
      "type": {
        "type": "record",
        "name": "CustomerMessage",
        "namespace": "com.simple.eos",
        "fields": [
          {"name": "message_id",  "type": "string"},
          {"name": "customer_id", "type": "string"},
          {"name": "name",        "type": "string"},
          {"name": "email",       "type": "string"},
          {"name": "action",      "type": {"type": "enum", "name": "Action", "symbols": ["CREATE", "UPDATE"]}}
        ]
      }
    },
    {"name": "failed_at_step",  "type": "string"},
    {"name": "reason",          "type": "string"},
    {"name": "idempotency_key", "type": "string"},
    {"name": "retry_count",     "type": "long"}
  ]
}`
