package main

import (
	"context"
	"encoding/json"
	"events_analytics_platform/cmd/util"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"log"
	"strings"
	"time"
)

func main() {
	brokers := strings.Split(util.GetEnv("KAFKA_BROKERS", "kafka1:29092"), ",")
	consumer := kafka.NewConsumer(brokers, "aggregator-group", "processed-user-events")
	producer := kafka.NewProducer(brokers, "user-order-aggregates")

	state := make(map[string]*models.UserOrderAggregates)
	ctx := context.Background()

	log.Println("Aggregator started...")

	for {
		msg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		var event models.NormalizedOrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("invalid event:", err)
			continue
		}

		agg, exists := state[event.UserID]
		if !exists {
			agg = &models.UserOrderAggregates{
				UserID: event.UserID,
			}
			state[event.UserID] = agg
		}

		agg.TotalOrders++
		agg.TotalAmount += event.Amount
		agg.LastOrderAt = event.ProcessedAt
		agg.UpdatedAt = time.Now().UTC()

		err = producer.Publish(ctx, event.UserID, agg)
		if err != nil {
			log.Printf("failed publishing order aggregation of order id#%d: %v\n", event.OrderID, err)
			continue
		}
		log.Println(agg)
	}
}
