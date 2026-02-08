package main

import (
	"context"
	"encoding/json"
	"events_analytics_platform/cmd/util"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"events_analytics_platform/internal/mongo"
	"events_analytics_platform/internal/services"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	brokers := strings.Split(util.GetEnv("KAFKA_BROKERS", "kafka1:29092"), ",")
	consumer := kafka.NewConsumer(brokers, "aggregator-group", "processed-user-events")
	producer := kafka.NewProducer(brokers, "user-order-aggregates")
	dqlProducer := kafka.NewProducer(brokers, "dead-letter-events")
	hourlyProducer := kafka.NewProducer(brokers, "user-order-aggregates-hourly")
	dailyProducer := kafka.NewProducer(brokers, "user-order-aggregates-daily")

	defer func() {
		err := producer.Close()
		if err != nil {
			log.Fatal("error when closing producer:", err)
		}

		err = dqlProducer.Close()
		if err != nil {
			log.Fatal("error when closing aggregator dqlProducer:", err)
		}
	}()

	mongoClient, err := mongo.New(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal(err)
	}

	db := mongoClient.Database("analytics")
	aggregateRepo := mongo.NewAggregateRepo(db)
	processTracker := mongo.NewProcessTracker(db)
	AggregateRepoStorage := &mongo.AggregateRepo{
		Coll:          db.Collection("user_order_aggregates"),
		WindowAggrCol: db.Collection("user_order_window_aggregates"),
	}
	windowedAggregatorRepo := services.NewWindowedAggregator(AggregateRepoStorage, hourlyProducer, dailyProducer)

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
			if err = util.SendDLQ(ctx, dqlProducer, string(msg.Key), err, &event); err != nil {
				log.Println(err)
			}
			continue
		}

		processed, err := processTracker.IsProcessed(ctx, event.EventID)
		if err != nil {
			log.Println("process tracker error:", err)
			continue
		}

		if processed {
			log.Printf("duplicate event %s skipped\n", event.EventID)
			err = consumer.CommitMessages(ctx, msg)
			if err != nil {
				log.Println("error when commiting the processed tracker:", err)
			}
			continue
		}

		var success bool
		for i := 0; i < 3; i++ {
			if err := aggregateRepo.Update(ctx, event); err == nil {
				success = true
				break
			}
			time.Sleep(time.Second * time.Duration(i+1))
		}

		if !success {
			if err := util.SendDLQ(ctx, dqlProducer, string(msg.Key), fmt.Errorf("aggregation update failed %d times", 3), &event); err != nil {
				log.Println(err)
			}
			continue
		}

		err = processTracker.MarkProcessed(ctx, event.EventID)
		if err != nil {
			log.Println("mark process error:", err)
			continue
		}

		err = consumer.CommitMessages(ctx, msg)
		if err != nil {
			log.Println("error when commiting the aggregation repo:", err)
			continue
		}

		userTotalOrder, err := aggregateRepo.UserTotalOrder(ctx, event.UserID)
		if err != nil {
			log.Println("error when getting user total order from aggregation:", err)
			continue
		}

		orderAggr := models.UserOrderAggregates{
			UserID:      event.UserID,
			TotalOrders: userTotalOrder.TotalOrders,
			TotalAmount: float64(userTotalOrder.TotalAmount),
			LastOrderAt: time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}

		err = producer.Publish(ctx, string(msg.Key), orderAggr)
		if err != nil {
			log.Println("error in publishing aggregate events:", err)
			continue
		}

		err = windowedAggregatorRepo.Process(ctx, models.NormalizedOrderEvent{
			EventID:     event.EventID,
			UserID:      event.UserID,
			EventType:   event.EventType,
			Source:      event.Source,
			OrderID:     event.OrderID,
			Amount:      event.Amount,
			ReceivedAt:  event.ReceivedAt,
			ProcessedAt: event.ProcessedAt,
		})
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("orderAggr ===>", orderAggr)
	}
}
