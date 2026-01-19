package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// This map is our in-memory "Table" of products.
// It is protected by a mutex to handle concurrent access safely.
var products = make(map[string]string)
var mu sync.RWMutex

// Function to continuously update our product table from a Kafka topic
func consumeProductUpdates() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "product-updates",
		GroupID: "product-table-builder",
	})
	defer reader.Close()

	log.Println("Product table consumer started...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("could not read product update: %v", err)
			break
		}
		// The key is the product ID, the value is the product name
		productID := string(msg.Key)
		productName := string(msg.Value)

		mu.Lock()
		products[productID] = productName
		mu.Unlock()

		log.Printf("UPDATED TABLE: Product ID '%s' is now '%s'", productID, productName)
	}
}

func main() {
	// Start a separate goroutine to build our product table in the background
	go consumeProductUpdates()

	// --- Main Stream Processor Logic ---
	orderConsumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "simple-orders",
		GroupID: "order-enrichment-group",
	})
	defer orderConsumer.Close()

	producer := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
	}
	defer producer.Close()

	log.Println("Order enrichment processor started...")

	ctx := context.Background()
	for {
		// Read an incoming order
		orderMsg, err := orderConsumer.ReadMessage(ctx)
		if err != nil {
			break
		}

		// The order value is just the product ID it's for
		productID := string(orderMsg.Value)
		log.Printf("Received order for product ID: %s", productID)

		// --- The JOIN operation ---
		// We do a lookup in our in-memory table.
		mu.RLock()
		productName, ok := products[productID]
		mu.RUnlock()

		if !ok {
			// If we don't know the product, we can't enrich the order.
			log.Printf("Product ID '%s' not found in table. Skipping order.", productID)
			continue
		}

		// --- Enrich the order ---
		enrichedOrder := map[string]interface{}{
			"order_id":     string(orderMsg.Key),
			"product_id":   productID,
			"product_name": productName, // The data we joined!
			"timestamp":    time.Now().Unix(),
		}
		enrichedValue, _ := json.Marshal(enrichedOrder)

		// Produce the enriched result
		err = producer.WriteMessages(ctx, kafka.Message{
			Topic: "enriched-orders",
			Key:   orderMsg.Key,
			Value: enrichedValue,
		})

		if err != nil {
			log.Printf("failed to write enriched order: %v", err)
		} else {
			log.Printf("Successfully produced enriched order for key %s", string(orderMsg.Key))
		}
	}
}
