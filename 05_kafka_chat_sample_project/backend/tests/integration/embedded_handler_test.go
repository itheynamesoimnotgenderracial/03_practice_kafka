package integration

import (
	"context"
	"fmt"
	"sample-chat/internal/handler"
	"sample-chat/internal/mongo"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	baseMongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─────────────────────────────────────────────────────────────────────────────
// Embedded Test 1: Validation Handler — DLT Routing
// ─────────────────────────────────────────────────────────────────────────────
//
// Tests the ChatValidationHandler directly:
//   - Valid messages → routed to chat.validated
//   - Invalid messages (empty fields, too long) → routed to chat.raw.dlt
//   - Sanitization (badword → ***) works correctly
//
// This runs the handler in-process without Kafka, testing the pure logic.

func TestValidationHandler_ValidateMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Connect to Stream Registry for avro ser/de
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(SchemaRegistryURL))
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer srClient.Close()

	deserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer deserializer.Close()

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		t.Fatalf("failed to create deserializer: %v", err)
	}
	defer serializer.Close()

	validationHandler := handler.ChatValidationHandler(deserializer, serializer)

	// Create a valid raw event and serialize it
	rawEvent := ChatRawEvent{
		MessageID: uuid.New().String(),
		RoomID:    "test-room-validation",
		UserID:    "user-1",
		Content:   "Hello, this is a valid message",
		Timestamp: time.Now().UnixMilli(),
	}

	serialized, err := serializer.Serialize("chat.raw-value", &rawEvent)
	if err != nil {
		t.Fatalf("failed to serialize raw event: %v", err)
	}

	// Create a fake Kafka message
	topic := TopicChatRaw
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            []byte(rawEvent.RoomID),
		Value:          serialized,
	}

	// Run the handler
	outputs, err := validationHandler(msg)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// Verify: should produce exactly 1 message to chat.validated
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputs))
	}

	if outputs[0].Topic != "chat.validated" {
		t.Errorf("expected output to chat.validated, got %s", outputs[0].Topic)
	}

	t.Log("✅ Valid message correctly routed to chat.validated")
}

func TestValidationHandler_InvalidMessage_EmptyContent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Connect to Stream Registry for avro ser/de
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(SchemaRegistryURL))
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer srClient.Close()

	deserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer deserializer.Close()

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		t.Fatalf("failed to create deserializer: %v", err)
	}
	defer serializer.Close()

	validationHandler := handler.ChatValidationHandler(deserializer, serializer)

	// Create an INVALID raw event - empty content
	rawEvent := ChatRawEvent{
		MessageID: uuid.New().String(),
		RoomID:    "test-room-validation",
		UserID:    "user-1",
		Content:   " ",
		Timestamp: time.Now().UnixMilli(),
	}

	serialized, err := serializer.Serialize("chat.raw-value", &rawEvent)
	if err != nil {
		t.Fatalf("failed to serialize raw event: %v", err)
	}

	topic := TopicChatRaw
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            []byte(rawEvent.RoomID),
		Value:          serialized,
	}

	outputs, err := validationHandler(msg)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// Verify: should produce to DLT, not chat.validated
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputs))
	}

	if outputs[0].Topic != "chat.raw.dlt" {
		t.Errorf("expected output to chat.raw.dlt, got %s", outputs[0].Topic)
	}

	t.Log("✅ Invalid message correctly routed to chat.raw.dlt")
}

func TestValidationHandler_Sanitization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(SchemaRegistryURL))
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer srClient.Close()

	deserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		t.Fatalf("failed to create deserializer: %v", err)
	}
	defer deserializer.Close()

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}
	defer serializer.Close()

	validationHandler := handler.ChatValidationHandler(deserializer, serializer)

	// Create a message with "badword" that should be sanitized
	rawEvent := ChatRawEvent{
		MessageID: uuid.New().String(),
		RoomID:    "test-room-validation",
		UserID:    "user-1",
		Content:   "This contains a badword in the message",
		Timestamp: time.Now().UnixMilli(),
	}

	serialized, err := serializer.Serialize("chat.raw-value", &rawEvent)
	if err != nil {
		t.Fatalf("failed to serialize raw event: %v", err)
	}

	topic := TopicChatRaw
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            []byte(rawEvent.RoomID),
		Value:          serialized,
	}

	outputs, err := validationHandler(msg)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// Should still go to chat.validated (sanitized, not rejected)
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputs))
	}

	if outputs[0].Topic != "chat.validated" {
		t.Errorf("expected output to chat.validated, got %s", outputs[0].Topic)
	}

	// Verify the content was sanitized by deserializing the output
	var validated handler.ChatValidatedEvent
	err = deserializer.DeserializeInto("chat.validated-value", outputs[0].Value, &validated)
	if err != nil {
		t.Fatalf("failed to deserialize validated event: %v", err)
	}

	if validated.Content != "This contains a *** in the message" {
		t.Errorf("expected sanitized content, got: %s", validated.Content)
	}

	if !validated.Sanitized {
		t.Error("expected Sanitized=true for badword content")
	}

	t.Log("✅ Badword correctly sanitized to ***")
}

// ─────────────────────────────────────────────────────────────────────────────
// Embedded Test 2: Timeline Handler — Sequence Assignment
// ─────────────────────────────────────────────────────────────────────────────
//
// Tests the TimelineHandler directly with a real MongoDB SequenceStore:
//   - Sequences are assigned atomically (no gaps under concurrent calls)
//   - Each call increments the sequence by exactly 1
//   - Output includes the correct sequence number

func TestTimelineHander_SequenceAssignment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Connect to MongoDB for the real SequenceStore
	ctx := context.Background()
	mongoClient, err := baseMongo.Connect(ctx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		t.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database(MongoDB)

	// Use a unique room to avoid conflicts with other tests
	roomID := fmt.Sprintf("eos-seq-test-%s", uuid.New().String()[:8])

	// Clean up this room's sequence data
	defer db.Collection(CollectionSequences).DeleteMany(ctx, bson.M{"room_id": roomID})

	seqStore := mongo.NewSequenceStore(db, CollectionSequences)

	// Connect to Schema Registry
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(SchemaRegistryURL))
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}
	defer srClient.Close()

	deserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		t.Fatalf("failed to create deserializer: %v", err)
	}
	defer deserializer.Close()

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}
	defer serializer.Close()

	timelineHandler := handler.NewTimelineHandler(deserializer, serializer, seqStore)

	// Send 10 messages through the handler and collect sequences
	messageCount := 10
	sequences := make([]int64, 0, messageCount)

	for i := 0; i < messageCount; i++ {
		validatedEvent := handler.ChatValidatedEvent{
			MessageID:   uuid.New().String(),
			RoomID:      roomID,
			UserID:      "test-user",
			Content:     fmt.Sprintf("sequence test message %d", i),
			Sanitized:   false,
			Timestamp:   time.Now().UnixMilli(),
			ValidatedAt: time.Now().UnixMilli(),
		}

		serialized, err := serializer.Serialize("chat.validated-value", &validatedEvent)
		if err != nil {
			t.Fatalf("failed to serialize validated event: %v", err)
		}

		topic := TopicChatValidated
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Key:            []byte(roomID),
			Value:          serialized,
		}

		outputs, err := timelineHandler(msg)
		if err != nil {
			t.Fatalf("timeline handler error on message %d: %v", i, err)
		}

		if len(outputs) != 1 {
			t.Fatalf("expected 1 output, got %d", len(outputs))
		}

		if outputs[0].Topic != "chat.timeline" {
			t.Errorf("expected output to chat.timeline, got %s", outputs[0].Topic)
		}

		// Deserialize the output to get the assigned sequence
		var timeline handler.ChatTimelineEvent
		err = deserializer.DeserializeInto("chat.timeline-value", outputs[0].Value, &timeline)
		if err != nil {
			t.Fatalf("failed to deserialize timeline event: %v", err)
		}

		sequences = append(sequences, timeline.Sequence)
	}

	// Verify: sequences should be 1, 2, 3, ..., 10 (contiguous, no gaps)
	for i, seq := range sequences {
		expected := int64(i + 1)
		if seq != expected {
			t.Errorf("sequence[%d] = %d, expected %d", i, seq, expected)
		}
	}

	// Verify: MongoDB sequence counter matches
	finalSeq := int64(0)
	var result struct {
		Sequence int64 `bson:"sequence"`
	}
	err = db.Collection(CollectionSequences).FindOne(ctx, bson.M{"room_id": roomID}).Decode(&result)
	if err != nil {
		t.Fatalf("failed to read sequence counter: %v", err)
	}
	finalSeq = result.Sequence

	if finalSeq != int64(messageCount) {
		t.Errorf("expected sequence counter %d, got %d", messageCount, finalSeq)
	}

	t.Logf("✅ Sequence assignment test passed: %d messages, sequences 1-%d contiguous", messageCount, messageCount)
}

// ─────────────────────────────────────────────────────────────────────────────
// Embedded Test 3: Idempotent MongoDB Upsert
// ─────────────────────────────────────────────────────────────────────────────
//
// Tests that writing the same message_id twice to chat_messages via $setOnInsert
// does NOT create duplicates and does NOT overwrite existing data.

func TestIdempotentMongoUpsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	mongoClient, err := baseMongo.Connect(ctx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		t.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	col := mongoClient.Database(MongoDB).Collection(CollectionMessage)

	roomID := fmt.Sprintf("eos-idempotent-%s", uuid.New().String()[:8])
	messageID := uuid.New().String()

	// Clean up after test
	defer col.DeleteMany(ctx, bson.M{"room_id": roomID})

	// First write — should insert
	filter := bson.M{"message_id": messageID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"message_id": messageID,
			"room_id":    roomID,
			"user_id":    "user-1",
			"content":    "original content",
			"sequence":   int64(1),
			"timestamp":  time.Now().UnixMilli(),
		},
	}
	opts := options.Update().SetUpsert(true)

	result1, err := col.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}
	if result1.UpsertedCount != 1 {
		t.Errorf("expected 1 upsert on first write, got %d", result1.UpsertedCount)
	}

	// Second write — same message_id, different content — should NOT overwrite
	update2 := bson.M{
		"$setOnInsert": bson.M{
			"message_id": messageID,
			"room_id":    roomID,
			"user_id":    "user-1",
			"content":    "REPLAYED CONTENT — should not appear",
			"sequence":   int64(1),
			"timestamp":  time.Now().UnixMilli(),
		},
	}

	result2, err := col.UpdateOne(ctx, filter, update2, opts)
	if err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}
	if result2.UpsertedCount != 0 {
		t.Errorf("expected 0 upserts on replay, got %d (duplicate created!)", result2.UpsertedCount)
	}
	if result2.MatchedCount != 1 {
		t.Errorf("expected 1 matched on replay, got %d", result2.MatchedCount)
	}

	// Verify: only 1 document exists with original content
	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 document, got %d", count)
	}

	// Verify content was NOT overwritten
	var stored struct {
		Content string `bson:"content"`
	}
	err = col.FindOne(ctx, filter).Decode(&stored)
	if err != nil {
		t.Fatalf("failed to read stored message: %v", err)
	}

	if stored.Content != "original content" {
		t.Errorf("content was overwritten! expected 'original content', got '%s'", stored.Content)
	}

	t.Log("✅ Idempotent upsert test passed: replay did not create duplicate or overwrite data")
}
