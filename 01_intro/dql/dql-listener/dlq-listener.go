package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	dqlTopic    = "orders-dql"
	dlqGroupdID = "dlq-handler-group"
	brokerURL   = "localhost:9092"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerURL},
		Topic:   dqlTopic,
		GroupID: dlqGroupdID,
	})
	defer reader.Close()
	log.Println("Starting DQL monitor for topic:", dqlTopic)

	ctx := context.Background()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			panic(err)
		}
		log.Printf("DLQ received {%s} offset %d: %s", string(m.Key), m.Offset, string(m.Value))
		reader.CommitMessages(ctx, m)
	}
}
