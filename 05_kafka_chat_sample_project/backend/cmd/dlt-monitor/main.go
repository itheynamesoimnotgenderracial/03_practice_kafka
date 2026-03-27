package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"sample-chat/internal/dlt"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	collectionDLT = "dlt_events"
)

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27071")

	ctx := context.Background()

	// ─── Kafka consumer for DLT topic ───
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group-id":          "dlt-monitor-group",
		"auto.offset-reset": "earliest",
	})
	if err != nil {
		log.Fatal("failed to subscribe to DLT topic:", err)
	}

	// ─── MongoDB connection ───
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to MongoDB:", err)
	}
	defer func() {
		err = mongoClient.Disconnect(ctx)
		if err != nil {
			log.Fatal("failed to subscribe to DLT topic:", err)
		}
	}()

	db := mongoClient.Database("chat")
	col := db.Collection(collectionDLT)

	// Create indexes for DLT collection
	ensureDLTIndexes(ctx, col)

	// ─── Graceful shutdown ───
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("🔍 DLT Monitor is watching chat.raw.dlt...")

	run := true
	for run {
		select {
		case sig := <-sigChan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			processDLTMessage(ctx, col, msg)
		}
	}
}

// processDLTMessage logs the dead-lettered message and persists it to MongoDB.
func processDLTMessage(ctx context.Context, col *mongo.Collection, msg *kafka.Message) {
	event := dlt.NewDLTEventFromMessage(msg)

	// ─── Structured logging ───
	logEntry := map[string]interface{}{
		"level":          "WARN",
		"service":        "dlt-monitor",
		"message_id":     event.MessageID,
		"original_topic": event.OriginalTopic,
		"error_type":     event.ErrorType,
		"failure_reason": event.FailureReason,
		"retry_count":    event.RetryCount,
		"partition":      event.Partition,
		"offset":         event.Offset,
		"key":            event.Key,
		"failed_at":      time.UnixMilli(event.FailedAt).UTC().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(logEntry)
	fmt.Printf("☠️  DLT EVENT: %s\n", string(logJSON))

	// ─── Persist to MongoDB (upsert by message_id to prevent duplicates) ───
	filter := bson.M{"message_id": event.MessageID, "failed_at": event.FailedAt}
	update := bson.M{
		"$setOnInsert": event,
	}
	opts := options.Update().SetUpsert(true)

	_, err := col.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Printf("Failed to persist DLT event to MongoDB: %v", err)
		return
	}

	fmt.Printf("📝 Stored DLT event: message_id=%s error_type=%s retry_count=%d\n",
		event.MessageID,
		event.ErrorType,
		event.RetryCount,
	)
}

// ensureDLTIndexes creates MongoDB indexes for efficient DLT queries.
func ensureDLTIndexes(ctx context.Context, col *mongo.Collection) {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "message_id", Value: 1}},
			Options: options.Index(),
		},
		{
			Keys:    bson.D{{Key: "error_type", Value: 1}},
			Options: options.Index(),
		},
		{
			Keys:    bson.D{{Key: "created_at", Value: -1}},
			Options: options.Index(),
		},
		{
			Keys:    bson.D{{Key: "replayed", Value: 1}},
			Options: options.Index(),
		},
	}

	_, err := col.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Printf("Warning: failed to create DLT indexes: %v", err)
	}
}
