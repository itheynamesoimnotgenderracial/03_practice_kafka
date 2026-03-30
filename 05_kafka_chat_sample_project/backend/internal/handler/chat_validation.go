package handler

import (
	"fmt"
	"sample-chat/internal/dlt"
	"sample-chat/internal/kafka"
	"strings"
	"time"

	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

const MaxContentLength = 4096

type ChatRawEvent struct {
	MessageID string `json:"message_id" avro:"message_id" bson:"message_id"`
	RoomID    string `json:"room_id" avro:"room_id" bson:"room_id"`
	UserID    string `json:"user_id" avro:"user_id" bson:"user_id"`
	Content   string `json:"content" avro:"content" bson:"content"`
	Timestamp int64  `json:"timestamp" avro:"timestamp" bson:"timestamp"`
}

type ChatValidatedEvent struct {
	MessageID   string `json:"message_id" avro:"message_id" bson:"message_id"`
	RoomID      string `json:"room_id" avro:"room_id" bson:"room_id"`
	UserID      string `json:"user_id" avro:"user_id" bson:"user_id"`
	Content     string `json:"content" avro:"content" bson:"content"`
	Sanitized   bool   `json:"sanitized" avro:"sanitized" bson:"sanitized"`
	Timestamp   int64  `json:"timestamp" avro:"timestamp" bson:"timestamp"`
	ValidatedAt int64  `json:"validated_at" avro:"validated_at" bson:"validated_at"`
}

type ChatDLTEvent struct {
	MessageID string `json:"message_id"`
	RoomID    string `json:"room_id"`
	UserID    string `json:"user_id"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
	Reason    string `json:"reason"`
	FailedAt  int64  `json:"failed_at"`
}

type ChatTimelineEvent struct {
	MessageID string `avro:"message_id" json:"message_id" bson:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id" bson:"room_id"`
	UserID    string `avro:"user_id" json:"user_id" bson:"user_id"`
	Content   string `avro:"content" json:"content" bson:"content"`
	Sequence  int64  `avro:"sequence" json:"sequence" bson:"sequence"`
	Timestamp int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}

func ChatValidationHandler(
	deserializer *avro.GenericDeserializer,
	serializer *avro.GenericSerializer,
) kafka.Handler {
	return func(msg *baseKafka.Message) ([]kafka.ProducerMessage, error) {
		var raw ChatRawEvent

		err := deserializer.DeserializeInto("chat.raw-value", msg.Value, &raw)
		if err != nil {
			destTopic, retryCount := dlt.RouteToRetryOrDLT(msg, dlt.ErrorTypeDeserialization)
			failMsg := dlt.BuildFailureMessage(
				msg,
				destTopic,
				retryCount,
				fmt.Sprintf("deserialization failed: %v\n", err),
				dlt.ErrorTypeDeserialization,
				"",
			)
			return []kafka.ProducerMessage{{
				Topic:   destTopic,
				Key:     failMsg.Key,
				Value:   failMsg.Value,
				Headers: failMsg.Headers,
			}}, nil

			// DONT DELETE!!!
			// temporary — force transient error for testing
			// ================ **** ================
			// return []kafka.ProducerMessage{{
			// 	Topic: dlt.TopicChatRawRetry,
			// 	Key:   msg.Key,
			// 	Value: msg.Value,
			// 	Headers: dlt.BuildFailureMessage(msg, dlt.TopicChatRawRetry,
			// 		dlt.GetRetryCount(msg)+1,
			// 		"simulated transient error",
			// 		dlt.ErrorTypeTransient, "").Headers,
			// }}, nil
		}

		validationErrors := make([]string, 0, 6)

		if strings.TrimSpace(raw.MessageID) == "" {
			validationErrors = append(validationErrors, "message_id is required")
		}
		if strings.TrimSpace(raw.RoomID) == "" {
			validationErrors = append(validationErrors, "room_id is required")
		}
		if strings.TrimSpace(raw.UserID) == "" {
			validationErrors = append(validationErrors, "user_id is required")
		}
		if strings.TrimSpace(raw.Content) == "" {
			validationErrors = append(validationErrors, "content must not be empty")
		}
		if len([]rune(raw.Content)) > MaxContentLength {
			validationErrors = append(validationErrors, fmt.Sprintf("content exceeds max length %d", MaxContentLength))
		}
		if raw.Timestamp == 0 {
			validationErrors = append(validationErrors, "timestamp is required")
		}

		if len(validationErrors) > 0 {
			reason := strings.Join(validationErrors, "; ")
			destTopic, retryCount := dlt.RouteToRetryOrDLT(msg, dlt.ErrorTypeValidation)
			failMsg := dlt.BuildFailureMessage(
				msg,
				destTopic,
				retryCount,
				reason,
				dlt.ErrorTypeValidation,
				raw.MessageID,
			)

			return []kafka.ProducerMessage{{
				Topic:   destTopic,
				Key:     failMsg.Key,
				Value:   failMsg.Value,
				Headers: failMsg.Headers,
			}}, nil
		}

		sanitizedContent, wasSanitized := sanitize(raw.Content)
		validated := ChatValidatedEvent{
			MessageID:   raw.MessageID,
			RoomID:      raw.RoomID,
			UserID:      raw.UserID,
			Content:     sanitizedContent,
			Sanitized:   wasSanitized,
			Timestamp:   raw.Timestamp,
			ValidatedAt: time.Now().UnixMilli(),
		}

		valueBytes, err := serializer.Serialize("chat.validated-value", &validated)
		if err != nil {
			return nil, err
		}

		output := kafka.ProducerMessage{
			Topic: "chat.validated",
			Key:   []byte(raw.RoomID),
			Value: valueBytes,
		}

		return []kafka.ProducerMessage{output}, nil
	}
}

func sanitize(content string) (string, bool) {
	trimmed := strings.TrimSpace(content)
	if strings.Contains(strings.ToLower(trimmed), "badword") {
		trimmed = strings.ReplaceAll(trimmed, "badword", "***")
		return trimmed, true
	}
	return trimmed, false
}
