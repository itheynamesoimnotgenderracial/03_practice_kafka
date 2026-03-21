package main

import (
	"context"
	"encoding/json"
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
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	redisAddr := utils.GetEnv("REDIS_ADDR", "redis:6379")
	ctx := context.Background()

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

	leaderboardSerializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("failed to create avro serializer for leader board:", err)
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

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Println("Delivery failed:", ev.TopicPartition)
				}
			}
		}
	}()

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to mongoDB:", err)
	}

	collection := client.Database("chat").Collection("room_metrics")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() {
		err = rdb.Close()
		if err != nil {
			log.Fatal("failed at closing redis aggregator connection:")
		}
	}()

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

			roomPayload, err := json.Marshal(event)
			if err != nil {
				log.Println("Failed to marshal event for room fan-out:", err)
			} else {
				channel := fmt.Sprintf("chat_room:%s", event.RoomID)
				if pubErr := rdb.Publish(ctx, channel, roomPayload).Err(); pubErr != nil {
					log.Println("Failed to publish to room channel:", pubErr)
				}
			}

			// Hourly Aggregate
			hourlyWindowStart, _ := handler.ComputeHourlyWindow(event.Timestamp)
			dailyWindowStart, _ := handler.ComputeDailyWindow(event.Timestamp)

			processWindow(
				ctx,
				collection,
				producer,
				leaderboardSerializer,
				event,
				hourlyWindowStart.Unix(),
				"hourly",
			)

			processWindow(
				ctx,
				collection,
				producer,
				leaderboardSerializer,
				event,
				dailyWindowStart.Unix(),
				"daily",
			)

			fmt.Println("Processed event + published leaderboard ✅")
		}
	}

	client.Disconnect(ctx)
}

func processWindow(
	ctx context.Context,
	collection *mongo.Collection,
	producer *kafka.Producer,
	serializer *avro.GenericSerializer,
	event ChatTimelineEvent,
	windowStart int64,
	windowType string,
) {
	filter := map[string]interface{}{
		"room_id":      event.RoomID,
		"window_start": windowStart,
		"windowType":   windowType,
	}

	update := map[string]interface{}{
		"$inc": map[string]interface{}{
			"total_messages": 1,
		},
		"$addToSet": map[string]interface{}{"active_users": event.UserID},
		"$set": map[string]interface{}{
			"finalized":  true,
			"updated_at": time.Now().UTC(),
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Println("Mongo update failed:", err)
	}

	// Fetch updated doc
	var result struct {
		TotalMessages int64    `bson:"total_messages"`
		ActiveUsers   []string `bson:"active_users"`
	}

	collection.FindOne(ctx, filter).Decode(&result)

	leaderboardEvent := ChatLeaderboardEvent{
		RoomID:           event.RoomID,
		WindowStart:      windowStart,
		WindowType:       windowType,
		TotalMessages:    result.TotalMessages,
		ActiveUsersCount: int64(len(result.ActiveUsers)),
		Timestamp:        time.Now().Unix(),
	}
	serialized, err := serializer.Serialize(
		"chat.leaderboard-value",
		&leaderboardEvent,
	)
	if err != nil {
		log.Println("Failed to serialize leaderboard event:", err)
		return
	}

	topic := "chat.leaderboard"

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: serialized,
	}, nil)
	if err != nil {
		log.Println("Failed to chat leaderboard message:", err)
		return
	}
	fmt.Printf("Done processing %s data✅\n", windowType)
}
