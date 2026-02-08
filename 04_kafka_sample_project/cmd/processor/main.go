package main

import (
	"context"
	"encoding/json"
	"events_analytics_platform/cmd/util"
	"events_analytics_platform/internal/handler"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"events_analytics_platform/internal/mongo"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	brokers := strings.Split(util.GetEnv("KAFKA_BROKERS", "kafka2:29092"), ",")

	consumer := kafka.NewConsumer(brokers, "processor-grp", "raw-user-events")
	processedEventProducer := kafka.NewProducer(brokers, "processed-user-events")
	dlqProducer := kafka.NewProducer(brokers, "dead-letter-events")

	defer func() {
		err := processedEventProducer.Close()
		if err != nil {
			log.Fatal("error when closing processes event producer", err)
		}

		err = dlqProducer.Close()
		if err != nil {
			log.Fatal("error when closing processes dqlProducer", err)
		}
	}()

	mongoClient, err := mongo.New(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal(err)
	}

	collection := mongoClient.Database("analytics").Collection("events")
	ctx := context.Background()

	log.Println("processor started, waiting for messages...")

	for {
		msg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("read error: ", err)
			continue
		}

		var event models.AuditEvent
		var eventError models.EventError
		var normalizedEvent models.NormalizedOrderEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			eventError = models.EventError{
				Error:         fmt.Sprintf("invalid json, sending to DLQ: %v", err),
				OriginalEvent: models.AuditEvent{},
				FailedAt:      time.Now().UTC(),
			}
			_ = dlqProducer.Publish(ctx, string(msg.Key), eventError)
			continue
		}

		if err := handler.HandleMessage(ctx, &event, &normalizedEvent, collection); err != nil {
			eventError = models.EventError{
				Error:         fmt.Sprintf("processing error, sending to DLQ: %v", err),
				OriginalEvent: event,
				FailedAt:      time.Now().UTC(),
			}
			_ = dlqProducer.Publish(ctx, string(msg.Key), eventError)
			continue
		}

		err = processedEventProducer.Publish(ctx, string(msg.Key), normalizedEvent)
		if err != nil {
			log.Println("error in publishing processed events:", err)
			continue
		}

		log.Printf("%v %v\n", time.Now().Unix(), event)
	}
}
