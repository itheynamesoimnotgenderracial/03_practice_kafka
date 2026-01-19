package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	producer := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092", "localhost:9094", "localhost:9095"),
	}

	defer producer.Close()

	// Prepare the Request
	requestTopic := "fraud-check-request"
	replyTopic := "fraud-check-replies"
	correlationID := fmt.Sprintf("request-%d", time.Now().UnixNano())

	requestMessage := kafka.Message{
		Topic: requestTopic,
		Value: []byte("Check transaction for user: 456, amount: $123.45"),
		Headers: []kafka.Header{
			{Key: "correlation-id", Value: []byte(correlationID)},
			{Key: "reply-to-topic", Value: []byte(replyTopic)},
		},
	}

	fmt.Println("Sending request with correlation ID: ", correlationID)
	err := producer.WriteMessages(ctx, requestMessage)
	if err != nil {
		log.Fatal("failed to send request: ", err)
	}

	// wait for reply
	// Create a new reader to listen on the reply topic
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:   replyTopic,
		GroupID: fmt.Sprintf("payment-service-group-%s", correlationID),
	})

	defer consumer.Close()

	fmt.Println("Waiting for reply...")

	// Loop with a timeout for the read operation
	for {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		msg, err := consumer.ReadMessage(readCtx)
		if err != nil {
			fmt.Println("failed to receive reply: ", err)
			break
		}

		// checm if correlation ID matches
		for _, h := range msg.Headers {
			if h.Key == "correlation-id" && string(h.Value) == correlationID {
				fmt.Println("Receive matching reply! Response: ", string(msg.Value))
				return
			}
		}
	}
}
