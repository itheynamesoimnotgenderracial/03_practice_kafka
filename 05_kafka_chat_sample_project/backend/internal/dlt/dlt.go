package dlt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	// Kafka header keys for DLT metadata
	HeaderRetryCount    = "x-retry-count"
	HeaderOriginalTopic = "x-original-topic"
	HeaderFailureReason = "x-failure-reason"
	HeaderFailedAt      = "x-failed-at"
	HeaderErrorType     = "x-error-type"
	HeaderMessageID     = "x-message-id"

	// Error type classifications
	ErrorTypeValidation      = "validation"
	ErrorTypeDeserialization = "deserialization"
	ErrorTypePanic           = "panic"
	ErrorTypeTransient       = "transient"
	ErrorTypeUnknown         = "unknown"

	// Retry configuration
	MaxRetries        = 3
	RetryDelayBase    = 5 * time.Second
	TopicChatRawRetry = "chat.raw.retry"
	TopicChatRawDLT   = "chat.raw.dlt"
)

// ─── DLT Event (stored in MongoDB) ───

// DLTEvent represents a dead-lettered message stored in MongoDB for inspection.
type DLTEvent struct {
	MessageID     string `bson:"message_id" json:"message_id"`
	OriginalTopic string `bson:"original_topic" json:"original_topic"`
	ErrorType     string `bson:"error_type" json:"error_type"`
	FailureReason string `bson:"failure_reason" json:"failure_reason"`
	RawPayload    string `bson:"raw_payload" json:"raw_payload"` // base64 or JSON string of original value
	Key           string `bson:"key" json:"key"`
	RetryCount    int    `bson:"retry_count" json:"retry_count"`
	Offset        int64  `bson:"offset" json:"offset"`
	FailedAt      int64  `bson:"failed_at" json:"failed_at"`
	CreatedAt     int64  `bson:"created_at" json:"created_at"`
	ReplayedAt    int64  `bson:"replayed_at,omitempty" json:"replayed_at,omitempty"`
	Replayed      bool   `bson:"replayed" json:"replayed"`
	Partition     int32  `bson:"partition" json:"partition"`
}

// ─── Internal Helpers ───

func topicName(msg *kafka.Message) string {
	if msg.TopicPartition.Topic != nil {
		return *msg.TopicPartition.Topic
	}
	return "unknown"
}

// extractMessageID tries to parse message_id from a JSON payload.
// Returns empty string if the payload isn't valid JSON or doesn't contain message_id.
func extractMessage(value []byte) string {
	var parsed map[string]interface{}
	if err := json.Unmarshal(value, &parsed); err != nil {
		return ""
	}

	if id, ok := parsed["message_id"].(string); ok {
		return id
	}

	return ""
}

// ─── Header Helpers ───

// GetHeader extracts a string header value from a Kafka message.
func GetHeader(msg *kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// GetRetryCount extracts the retry count from message headers.
func GetRetryCount(msg *kafka.Message) int {
	val := GetHeader(msg, HeaderRetryCount)
	if val == "" {
		return 0
	}

	count, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return count
}

// SetRetryHeaders adds retry metadata headers to a Kafka message
func SetRetryHeaders(msg *kafka.Message, retryCount int, originalTopic, reason, errorType string) {
	// extract message_id from payload best-effort
	messageID := extractMessage(msg.Value)
	msg.Headers = append(msg.Headers,
		kafka.Header{Key: HeaderRetryCount, Value: []byte(strconv.Itoa(retryCount))},
		kafka.Header{Key: HeaderOriginalTopic, Value: []byte(originalTopic)},
		kafka.Header{Key: HeaderFailureReason, Value: []byte(reason)},
		kafka.Header{Key: HeaderFailedAt, Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))},
		kafka.Header{Key: HeaderErrorType, Value: []byte(errorType)},
		kafka.Header{Key: "x-message-id", Value: []byte(messageID)},
	)
}

// ─── Routing Logic ───

// RouteToRetryOrDLT decides whether a failed message should go to the retry topic
// (for transient errors) or directly to the DLT (for permanent errors or max retries exceeded).
//
// Returns: (topic, updatedRetryCount)
func RouteToRetryOrDLT(msg *kafka.Message, errorType string) (string, int) {
	currentRetry := GetRetryCount(msg)

	// Permanent errors always go straight to DLT
	if errorType == ErrorTypeValidation ||
		errorType == ErrorTypeDeserialization ||
		errorType == ErrorTypePanic {
		return TopicChatRawDLT, currentRetry
	}

	// Transient errors: retry if under max
	if currentRetry < MaxRetries {
		return TopicChatRawRetry, currentRetry + 1
	}

	// Max retries exceeded + DLT
	return TopicChatRawDLT, currentRetry
}

// BuildFailureMessage creates a new Kafka message destined for either retry or DLT.
// preserving the original key/value and adding failure metadata headers.
func BuildFailureMessage(original *kafka.Message, destTopic string, retryCount int, reason, errorType, messageID string) *kafka.Message {
	failMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &destTopic,
			Partition: kafka.PartitionAny,
		},
		Key:   original.Key,
		Value: original.Value,
	}

	SetRetryHeaders(failMsg, retryCount, topicName(original), reason, errorType)
	failMsg.Headers = append(failMsg.Headers, kafka.Header{Key: HeaderMessageID, Value: []byte(messageID)})
	return failMsg
}

//  ─── DLT Event Construction ───

// NewDLTEventFromMessage constructs a DLTEvent from kafka message arriving on the DLT topic.
func NewDLTEventFromMessage(msg *kafka.Message) DLTEvent {
	now := time.Now().UnixMilli()

	// Read message_id from header first, fall back to offset-based ID
	messageID := GetHeader(msg, HeaderMessageID)
	if messageID == "" {
		messageID = fmt.Sprintf("unknown-%d-%d", msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}

	failedAtStr := GetHeader(msg, HeaderFailedAt)
	failedAt, _ := strconv.ParseInt(failedAtStr, 10, 64)
	if failedAt == 0 {
		failedAt = now
	}

	return DLTEvent{
		MessageID:     messageID,
		OriginalTopic: GetHeader(msg, HeaderOriginalTopic),
		ErrorType:     GetHeader(msg, HeaderErrorType),
		FailureReason: GetHeader(msg, HeaderFailureReason),
		RetryCount:    GetRetryCount(msg),
		RawPayload:    string(msg.Value), // Store raw bytes as string (may be binary/Avro)
		Key:           string(msg.Key),
		Partition:     msg.TopicPartition.Partition,
		Offset:        int64(msg.TopicPartition.Offset),
		FailedAt:      failedAt,
		CreatedAt:     now,
		Replayed:      false,
	}
}
