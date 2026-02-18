package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"sample-chat/internal/handler"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ChatTimelineEvent struct {
	MessageID string `avro:"message_id" json:"message_id" bson:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id" bson:"room_id"`
	UserID    string `avro:"user_id" json:"user_id" bson:"user_id"`
	Content   string `avro:"content" json:"content" bson:"content"`
	Sequence  int64  `avro:"sequence" json:"sequence" bson:"sequence"`
	Timestamp int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "aggregator-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("failed to build aggregator consumer:", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	chatTimelineDeserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatal("failed to create avro deserializer:", err)
	}

	defer func() {
		err = c.Close()
		if err != nil {
			log.Fatal("failed at closing kafka new consumer connection:", err)
		}
		chatTimelineDeserializer.Close()
	}()

	err = c.Subscribe("chat.timeline", nil)
	if err != nil {
		log.Fatal("failed to subscribe to processed events:", err)
	}

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to mongoDB:", err)
	}

	collection := client.Database("chat").Collection("room_metrics")

	// Graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	fmt.Println("Service aggregator is now processing the data...")

	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			var event ChatTimelineEvent
			err = chatTimelineDeserializer.DeserializeInto("chat.timeline-value", msg.Value, &event)
			if err != nil {
				log.Println("Failed to deserialize avro:", err)
				continue
			}

			var update map[string]interface{}
			var opts *options.UpdateOptions

			// Hourly Aggregate
			hourlyWindowStart, _ := handler.ComputeHourlyWindow(event.Timestamp)
			dailyWindowStart, _ := handler.ComputeDailylyWindow(event.Timestamp)
			hourlyFilter := map[string]interface{}{
				"room_id":      event.RoomID,
				"window_start": hourlyWindowStart.Unix(),
				// "window_end":   hourlyWindowEnd,
			}

			update = map[string]interface{}{
				"$inc": map[string]interface{}{"total_messages": 1},
				"$set": map[string]interface{}{
					"finalized":  true,
					"updated_at": time.Now().UTC(),
				},
				"$addToSet": map[string]interface{}{"active_users": event.UserID},
			}

			opts = options.Update().SetUpsert(true)
			_, err = collection.UpdateOne(context.TODO(), hourlyFilter, update, opts)
			if err != nil {
				log.Println("Mongo update failed:", err)
			}

			// Daily Aggregate
			dailyFilter := map[string]interface{}{
				"room_id":      event.RoomID,
				"window_start": dailyWindowStart.Unix(),
				// "window_end":   dailyWindowEnd,
			}

			update = map[string]interface{}{
				"$inc": map[string]interface{}{"total_messages": 1},
				"$set": map[string]interface{}{
					"finalized":  true,
					"updated_at": time.Now().UTC(),
				},
				"$addToSet": map[string]interface{}{"active_users": event.UserID},
			}

			opts = options.Update().SetUpsert(true)
			_, err = collection.UpdateOne(context.TODO(), dailyFilter, update, opts)
			if err != nil {
				log.Println("Mongo update failed:", err)
			}
			fmt.Println("Done processing data✅")
		}
	}

	client.Disconnect(context.TODO())
}
