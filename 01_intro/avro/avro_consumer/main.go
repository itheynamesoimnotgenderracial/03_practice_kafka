package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

// UserProfile is the same struct definition.
type UserProfile struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Age    int    `json:"age"`
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "avro-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer c.Close()

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s", err)
	}

	// Create a new Avro deserializer.
	deserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Failed to create Avro deserializer: %s", err)
	}

	topic := "user-profile-avro-events"
	c.SubscribeTopics([]string{topic}, nil)
	log.Println("Consumer started. Waiting for Avro messages...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			// Implement clean shut down process
			run = false
		default:
			msg, err := c.ReadMessage(100)
			if err != nil {
				continue
			}

			// Create an empty struct to deserialize into.
			var user UserProfile
			// The deserializer handles fetching the schema and decoding the data.
			err = deserializer.DeserializeInto(topic, msg.Value, &user)
			if err != nil {
				log.Printf("Failed to deserialize message: %v\n", err)
				continue
			}

			fmt.Printf("Successfully deserialized message: %+v\n", user)
		}
	}
}
