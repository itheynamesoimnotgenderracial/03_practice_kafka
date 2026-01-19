package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// processMessage simulates our business logic.
// It will return an error if the message contains "fail".
func processMessage(msg kafka.Message) error {
	log.Printf("Attempting to process message: %s\n", string(msg.Value))
	if strings.Contains(string(msg.Value), "fail") {
		return fmt.Errorf("this is a poison pill message")
	}
	log.Println("Message processed successfully!")
	return nil
}

func main() {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "incoming-orders",
		GroupID: "resilient-processor-group",
	})
	defer consumer.Close()

	producer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Balancer: &kafka.LeastBytes{},
	}
	defer producer.Close()

	dlqTopic := "incoming-orders-dlq"
	maxRetries := 3

	log.Println("Resilient processor started...")

	ctx := context.Background()
	for {
		msg, err := consumer.ReadMessage(ctx)
		if err != nil {
			break
		}

		var processingError error
		// --- Retry Loop ---
		for i := 0; i < maxRetries; i++ {
			processingError = processMessage(msg)
			if processingError == nil {
				break // Success, exit the retry loop
			}
			log.Printf("Failed to process message (attempt %d/%d): %v\n", i+1, maxRetries, processingError)
			time.Sleep(2 * time.Second) // Wait before retrying
		}

		// --- DLQ Logic ---
		if processingError != nil {
			log.Printf("All retries failed. Sending message to Dead-Letter Queue (DLQ): %s\n", dlqTopic)

			// Add error information to the message headers before sending to DLQ
			msg.Headers = append(msg.Headers, kafka.Header{
				Key:   "error-reason",
				Value: []byte(processingError.Error()),
			})

			msg.Topic = dlqTopic // Change the topic to the DLQ topic

			// Produce the failed message to the DLQ
			if err := producer.WriteMessages(ctx, msg); err != nil {
				log.Printf("FATAL: Could not write to DLQ: %v\n", err)
			}
		}
	}
}
