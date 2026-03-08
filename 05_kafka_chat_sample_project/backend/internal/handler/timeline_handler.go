package handler

import (
	"context"
	"fmt"
	"log"
	"sample-chat/internal/kafka"
	"sample-chat/internal/mongo"
	"time"

	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

func NewTimelineHandler(
	deserializer *avro.GenericDeserializer,
	serializer *avro.GenericSerializer,
	seqStore *mongo.SequenceStore,
) kafka.Handler {
	return func(msg *baseKafka.Message) ([]kafka.ProducerMessage, error) {
		var validated ChatValidatedEvent

		err := deserializer.DeserializeInto("chat.validated-value", msg.Value, &validated)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize validated event: %w", err)
		}

		log.Printf("received validated event: message_id=%s room_id=%s\n", validated.MessageID, validated.RoomID)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		sequence, err := seqStore.NextSequence(ctx, validated.RoomID)
		if err != nil {
			return nil, fmt.Errorf("failed to assign sequence for room %s: %w", validated.RoomID, err)
		}

		log.Printf("assigned sequence=%d for room=%s message_id=%s\n",
			sequence, validated.RoomID, validated.MessageID)

		timeline := ChatTimelineEvent{
			MessageID: validated.MessageID,
			RoomID:    validated.RoomID,
			UserID:    validated.UserID,
			Content:   validated.Content,
			Sequence:  sequence,
			Timestamp: validated.Timestamp,
		}

		valueBytes, err := serializer.Serialize("chat.timeline-value", &timeline)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize timeline event to avro: %s", err)
		}

		output := kafka.ProducerMessage{
			Topic: "chat.timeline",
			Key:   []byte(validated.RoomID),
			Value: valueBytes,
		}

		return []kafka.ProducerMessage{output}, nil
	}
}
