package main

import (
	"context"
	"encoding/json"
	"log"
	"sample-chat/cmd/utils"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

func StartKafkaConsumer(ctx context.Context, redis *RedisClientStore) {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "leaderboard-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("failed to build leaderboard consumer:", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	leaderboardDeserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatal("failed to create avro deserializer:", err)
	}

	defer func() {
		err = c.Close()
		if err != nil {
			log.Fatal("failed at closing kafka new consumer connection:", err)
		}

		err = leaderboardDeserializer.Close()
		if err != nil {
			log.Fatal("failed at closing leaderboard deserialization registry:", err)
		}
	}()

	err = c.Subscribe("chat.leaderboard", nil)
	if err != nil {
		log.Fatal("failed to subscribe to processed events:", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			return
		default:
			m, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Println("Kafka error:", err)
				continue
			}

			var event ChatLeaderboardEvent
			err = leaderboardDeserializer.DeserializeInto("chat.leaderboard-value-value", m.Value, &event)
			if err != nil {
				log.Println("Failed to deserialize avro:", err)
				continue
			}

			if err := redis.UpdateScore(ctx, event.RoomID, event.TotalMessages); err != nil {
				log.Println("leaderboard redis update error:", err)
				continue
			}

			top, err := redis.GetTopN(ctx, 10)
			if err != nil {
				log.Println("redis fetch error:", err)
				continue
			}

			payload, err := json.Marshal(top)
			if err != nil {
				log.Println("failed marshalling payload:", err)
				continue
			}

			err = redis.PublishLeaderboard(ctx, payload)
			if err != nil {
				log.Println("failed in publishing leaderboard:", err)
				continue
			}

			log.Println("🚀 Leaderboard updated and published")
		}
	}
}
