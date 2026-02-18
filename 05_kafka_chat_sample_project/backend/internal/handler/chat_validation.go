package handler

import (
	"encoding/json"
	"errors"
	"sample-chat/internal/kafka"
	"strings"
	"time"

	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ChatRawEvent struct {
	MessageID string `json:"message_id"`
	RoomID    string `json:"room_id"`
	UserID    string `json:"user_id"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
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

func ChatValidationHandler(msg *baseKafka.Message) ([]kafka.ProducerMessage, error) {
	var raw ChatRawEvent

	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return nil, err
	}

	if raw.MessageID == "" || raw.RoomID == "" || raw.UserID == "" {
		return nil, errors.New("invalid message: missing required fields")
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

	valueBytes, err := json.Marshal(validated)
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

func sanitize(content string) (string, bool) {
	trimmed := strings.TrimSpace(content)
	if strings.Contains(strings.ToLower(trimmed), "badword") {
		trimmed = strings.ReplaceAll(trimmed, "badword", "***")
		return trimmed, true
	}
	return trimmed, false
}
