package consumer

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
	"simple-semo-eos/internal/serde"
)

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// BuildIdempotencyKey creates a deterministic key from message fields.
// Exported so export_test.go can expose it to the test package
func BuildIdempotencyKey(msg models.CustomerMessage) string {
	return fmt.Sprintf("%s::%s::%s", msg.MessageID, msg.CustomerID, msg.Action)
}

// dlqAvro is the avro-tagged wire representation of DLQMessage.
// RetryCount is int64 to match the Avro schema's "long" type (Go int is 64-bit
// but hamba/avro requires int64 for Avro long, not the untyped int kind).
type dlqAvro struct {
	OriginalPayload models.CustomerMessage `avro:"original_payload" json:"original_payload"`
	FailedAtStep    string                 `avro:"failed_at_step"   json:"failed_at_step"`
	Reason          string                 `avro:"reason"           json:"reason"`
	IdempotencyKey  string                 `avro:"idempotency_key"  json:"idempotency_key"`
	RetryCount      int64                  `avro:"retry_count"      json:"retry_count"`
}

func sendToDLQ(ctx context.Context, producer kafka.Producer, codec serde.Codec, dlqTopic string, dlq models.DLQMessage) {
	subject := dlqTopic + "-value"
	wire := dlqAvro{
		OriginalPayload: dlq.OriginalPayload,
		FailedAtStep:    dlq.FailedAtStep,
		Reason:          dlq.Reason,
		IdempotencyKey:  dlq.IdempotencyKey,
		RetryCount:      int64(dlq.RetryCount),
	}
	payload, err := codec.Serialize(subject, wire)
	if err != nil {
		log.Printf("[DLQ] x serialize failed: %v", err)
		return
	}
	if err := producer.Publish(dlqTopic, dlq.IdempotencyKey, payload); err != nil {
		log.Printf("[DLQ] x publish failed: %v", err)
		return
	}
	log.Printf("[DLQ] -> step=%s-15s reason=%s", dlq.FailedAtStep, dlq.Reason)
}
