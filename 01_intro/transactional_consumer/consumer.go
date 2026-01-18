package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	topic := "trnasactions-demo"
	consumerGroup := "transaction-demo-group"

	consumer, err := kafka.NewClient(
		kafka.SeedBrokers("localhost:9092"),
		kafka.ConsumerGroup(consumerGroup),
		kafka.ConsumeTopics(topic),
		kafka.FetchIsolationLevel(kafka.ReadCommitted()),
		kafka.RequireStableFetchOffsets(),
	)
	if err != nil {
		log.Fatal("Error creating kafka consumer:", err)
	}

	defer consumer.Close()

	fmt.Println("Transaction-safe consumer started...")

	for {
		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		fetches.EachPartition(func(ftp kafka.FetchTopicPartition) {
			if ftp.Err != nil {
				fmt.Println("Error in partition", ftp.Err)
				return
			}
			for _, rec := range ftp.Records {
				if rec.Attrs.IsTransactional() && !rec.Attrs.IsControl() && rec.ProducerEpoch == -1 {
					fmt.Println("Uncommitted record, skipping...")
					continue
				}
				fmt.Printf("Committed message: key=%s, value=%s, offset=%d\n", string(rec.Key), string(rec.Value), rec.Offset)
			}
		})
		time.Sleep(500 * time.Millisecond)
	}
}
