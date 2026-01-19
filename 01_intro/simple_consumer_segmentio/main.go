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
	groupID := "user-profiles-segio-consumer"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})

	defer func() {
		err := reader.Close()
		if err != nil {
			log.Fatalf("failed to close write: %v", err)
		}
	}()

	ctx := context.Background()
	for {

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message", err)
			continue
		}

		fmt.Printf("Received message: partition=%d, offset=%d, key=%s, value=%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		time.Sleep(500 * time.Millisecond)
	}
}
