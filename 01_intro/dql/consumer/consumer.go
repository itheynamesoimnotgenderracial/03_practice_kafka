package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	requestTopic = "orders"
	dqlTopic     = "orders-dql"
	groupdID     = "order-processing-group"
	brokerURL    = "localhost:9092"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerURL},
		Topic:   requestTopic,
		GroupID: groupdID,
	})
	defer reader.Close()

	// DLQ Producer setup (within the consumer app)
	dqlWriter := kafka.Writer{
		Addr:  kafka.TCP(brokerURL),
		Topic: dqlTopic,
	}
	defer dqlWriter.Close()

	log.Println("Starting consumer for topic: ", requestTopic)

	ctx := context.Background()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context cancelled", err)
				break
			}
			log.Println("Error fetching messages...", err)
			continue
		}

		log.Printf("Received message at offset %d: %s", m.Offset, string(m.Value))
		var order struct {
			OrderID string
			Items   int
		}

		var processingErr error
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			processingErr = fmt.Errorf("malformed message: %v", err)
		} else {
			log.Printf("Processing order %s with %d items...", order.OrderID, order.Items)

			if order.OrderID == "FAIL-ME-ORDER" {
				// send the message to DLQ
				processingErr = fmt.Errorf("simulated processing error for orderID: %s", order.OrderID)
			} else {
				// simulate some talk done
				time.Sleep(500 * time.Millisecond)
				processingErr = nil
			}
		}

		// DLQ Handling
		if processingErr != nil {
			log.Printf("ERROR: failed to process order %s: %v. Sending to DLQ", string(m.Key), processingErr)

			dlqPayload := map[string]interface{}{
				"original_message":   string(m.Value),
				"error":              processingErr.Error(),
				"timestamp":          time.Now().Format(time.RFC3339),
				"original_topic":     m.Topic,
				"original_partition": m.Partition,
				"original_offset":    m.Offset,
			}
			dlqValue, err := json.Marshal(dlqPayload)
			if err != nil {
				panic(err)
			}

			err = dqlWriter.WriteMessages(ctx, kafka.Message{
				Key:   m.Key,
				Value: dlqValue,
			})
			if err != nil {
				panic(err)
			}

			log.Println("Successfully sent message to DLQ!")
		} else {
			log.Println("Successfully processed order: ", string(m.Key))
		}

		// Offset committing
		// this is crucial. Whether the message succeded or was sent to the DLQ,
		// we commit its offset in the main topic so we don't process it again.
		err = reader.CommitMessages(ctx, m)
		if err != nil {
			panic(err)
		}
		log.Println("Committed the offset:", m.Offset)
	}
}
