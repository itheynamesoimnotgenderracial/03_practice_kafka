package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// The state of our application
// We use a simple in-memory map
// Key: user_id (string), Value: click_count(int64)
var clickCounts = make(map[string]int)

const (
	topic   = "user-clicks"
	topic2  = "clicks-per-window"
	URL     = "localhost:9092"
	groupID = "click-counter-group"
)

func main() {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{URL},
		Topic:   topic,
		GroupID: groupID,
	})
	defer consumer.Close()

	// Procuer setup (Transformer)
	producer := kafka.Writer{
		Addr: kafka.TCP(URL),
	}
	defer producer.Close()

	log.Println("Starting statelful click counter...")

	ctx := context.Background()

	// windowing logic
	// Creating a ticker that fires every 10seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// The 10 second has tumbled. It's now time to process the results
			log.Println("window closed. Processing and emitting results.")
			for userId, count := range clickCounts {
				// Create the result payload
				result := map[string]interface{}{
					"user_id":     userId,
					"click_count": count,
					"window_end":  time.Now().UTC().Format(time.RFC3339),
				}
				resultBytes, err := json.Marshal(result)
				if err != nil {
					log.Println("Failed to marshall results:", err)
					continue
				}

				msg := kafka.Message{
					Topic: topic2,
					Key:   []byte(userId),
					Value: resultBytes,
				}

				err = producer.WriteMessages(ctx, msg)
				if err != nil {
					log.Println("Failed to write results:", err)
				} else {
					log.Printf("Emitted the result for the user: %s, count: %d", userId, count)
				}
			}

			// IMPORTANT: Reset the state for the next window
			clickCounts = make(map[string]int)
			log.Println("************** STATE RESET FOR NEW WINDOW **************")
		default:
			// In between clicks, we continously read messages
			msg, err := consumer.ReadMessage(ctx)
			if err != nil {
				log.Println("count not read the message", err)
				return
			}

			// Aggregation Logic
			userId := string(msg.Key)
			clickCounts[userId]++
			log.Printf("Incremented count for user %s to %d", userId, clickCounts[userId])
		}
	}
}
