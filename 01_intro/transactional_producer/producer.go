package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	kafka "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	topic := "trnasactions-demo"
	producerId := strconv.FormatInt(int64(os.Getpid()), 10)

	client, err := kafka.NewClient(
		kafka.SeedBrokers("localhost:9092"),
		kafka.TransactionalID(producerId),
		kafka.DefaultProduceTopic(topic),
		kafka.RequiredAcks(kafka.AllISRAcks()),
	)
	if err != nil {
		log.Fatalln("Error creating kafka client", err)
	}
	defer client.Close()

	// Begine Transaction
	err = client.BeginTransaction()
	if err != nil {
		log.Fatalln("Couldn't start transaction:", err)
	}

	fmt.Println("Transaction Started!")

	// messages := []string{"Order Placed", "Payment Success", "fail now", "Inventory Updated"}
	messages := []string{"Order Placed", "Payment Success", "Inventory Updated"}

	for _, msg := range messages {
		// simulate failure
		if msg == "fail now" {
			rollback(ctx, client)
			os.Exit(1)
		}

		record := kafka.StringRecord(msg)
		result := client.ProduceSync(ctx, record)

		if result.FirstErr() != nil {
			log.Println("Failed to sent message, we aborting transaction...", result.FirstErr())
			rollback(ctx, client)
			return
		}

		fmt.Println("Message sent:", msg)
		time.Sleep(time.Second)
	}

	err = client.EndTransaction(ctx, kafka.TryCommit)
	if err != nil {
		log.Fatal("Commit failed:", err)
	}

	fmt.Println("Transaction committed successfully")
}

func rollback(ctx context.Context, client *kafka.Client) {
	fmt.Println("Rolling back tx...")
	err := client.AbortBufferedRecords(ctx)
	if err != nil {
		fmt.Println("error aborting buffered seconds: ", err)
		return
	}

	err = client.EndTransaction(ctx, kafka.TryAbort)
	if err != nil {
		fmt.Println("error roling back transaction: ", err)
		return
	}

	fmt.Println("Transaction ha been rolled back!")
}
