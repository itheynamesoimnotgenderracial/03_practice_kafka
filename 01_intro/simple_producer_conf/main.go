package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     "localhost:9095",
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"retries":                               10,
		"max.in.flight.requests.per.connection": 5,
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Println("❌ Delivery failed:", ev.TopicPartition)
				} else {
					fmt.Println("✅ Delivered to:", ev.TopicPartition)
				}
			}
		}
	}()

	// Topic to produce messages to
	topic := "user-profiles"

	// Message to send
	message := "Hello7 from Go"

	// Asynchronously produce a message
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, nil)
	if err != nil {
		panic(err)
	}

	p.Flush(5000)
	time.Sleep(1 * time.Second)
}
