package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "user-profiles-segio"

	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: topic,
		// Idempotence config
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
	}

	defer func() {
		err := writer.Close()
		if err != nil {
			log.Fatalf("failed to close write: %v", err)
		}
	}()

	fmt.Println("Producer started. Send messages to topic: ", topic)
	for i := range 10 {
		message := fmt.Sprintf("Message #%d for user", i)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := writer.WriteMessages(ctx, kafka.Message{
			Value: []byte(message),
		})
		if err != nil {
			log.Fatalf("failed to write message %d: %v", i, err)
		}

		fmt.Printf("Sent message: %s\n", message)

		// A small delay between messages
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Finished sending messages!")
}
