package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

// UserProfile is our Go struct that matches the Avro schema.
type UserProfile struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Age    int    `json:"age"`
}

func main() {
	// Connect to Kafka broker
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// Connect to the "Blueprint Library"
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s", err)
	}

	// Create a new Avro serializer for the message value.
	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Failed to create Avro serializer: %s", err)
	}

	topic := "user-profile-avro-events"

	// Create an instance of our Go struct
	user := UserProfile{
		UserID: "user-1000",
		Email:  "avro-user91@example.com",
		Age:    26,
	}

	// The serializer needs the schema string to create the payload.
	// It will automatically register the schema if it's new.
	payload, err := serializer.Serialize(topic, user)
	if err != nil {
		log.Fatalf("Failed to serialize payload: %s", err)
	}

	// Produce the message with the Avro payload
	deliveryChan := make(chan kafka.Event)
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Key:            []byte(user.UserID),
	}, deliveryChan)

	// 1. Wait for the CONFIRMATION of this specific message
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Successfully produced Avro message to topic %s\n", *m.TopicPartition.Topic)
	}
	close(deliveryChan)

	// 2. Before exiting, perform a FINAL SWEEP to send any other buffered messages.
	// This is your graceful shutdown guarantee.
	fmt.Println("Flushing producer...")
	p.Flush(5 * 1000) // Block for up to 5 seconds. Change the seconds value as per your project guidelines
}
