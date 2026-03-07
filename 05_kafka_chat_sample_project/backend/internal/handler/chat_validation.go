package handler

import (
	"encoding/json"
	"fmt"
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
	MessageID   string `json:"message_id"`
	RoomID      string `json:"room_id"`
	UserID      string `json:"user_id"`
	Content     string `json:"content"`
	Sanitized   bool   `json:"sanitized"`
	Timestamp   int64  `json:"timestamp"`
	ValidatedAt int64  `json:"validated_at"`
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

func ChatValidationHandler(
	deserializer *avro.GenericDeserializer,
	serializer *avro.GenericSerializer,
) kafka.Handler {
	return func(msg *baseKafka.Message) ([]kafka.ProducerMessage, error) {
		var raw ChatRawEvent

		err := deserializer.DeserializeInto("chat.raw-value", msg.Value, &raw)
		if err != nil {
			dlt := ChatDLTEvent{
				MessageID: "",
				RoomID:    "",
				UserID:    "",
				Content:   "",
				Timestamp: 0,
				Reason:    fmt.Sprintf("deserialization failed: %v\n", err),
				FailedAt:  time.Now().UnixMilli(),
			}
			dltBytes, _ := json.Marshal(dlt)
			return []kafka.ProducerMessage{{
				Topic: "chat.raw.dlt",
				Key:   msg.Key,
				Value: dltBytes,
			}}, nil
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
			dlt := ChatDLTEvent{
				MessageID: raw.MessageID,
				RoomID:    raw.RoomID,
				UserID:    raw.UserID,
				Content:   raw.Content,
				Timestamp: raw.Timestamp,
				Reason:    reason,
				FailedAt:  time.Now().UnixMilli(),
			}
			dltBytes, err := json.Marshal(dlt)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal DLT event: %w", err)
			}

			return []kafka.ProducerMessage{{
				Topic: "chat.raw.dlt",
				Key:   msg.Key,
				Value: dltBytes,
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
