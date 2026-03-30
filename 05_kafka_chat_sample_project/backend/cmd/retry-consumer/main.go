package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"sample-chat/internal/dlt"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "retry-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("failed to create retry consumer:", err)
	}

	err = consumer.Subscribe(dlt.TopicChatRawRetry, nil)
	if err != nil {
		log.Fatal("failed to subscribe to retry topic:", err)
	}

	// ─── Kafka producer to re-route messages ───
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		log.Fatal("failed to create retry producer:", err)
	}

	defer func() {
		producer.Close()
		if err := consumer.Close(); err != nil {
			log.Println("failed to close consumer:", err)
		}
	}()

	// Delivery report handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Retry delivery failed: %v — error: %v", ev.TopicPartition, ev.TopicPartition.Error)
				} else {

				}
				fmt.Printf("✅ Retry delivered to %s[%d]@%d\n",
					*ev.TopicPartition.Topic,
					ev.TopicPartition.Partition,
					ev.TopicPartition.Offset,
				)
			}
		}
	}()

	// ─── Graceful shutdown ───
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("🔄 Retry consumer is processing chat.raw.retry...")

	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			processRetryMessage(producer, msg)
		}
	}
}

func processRetryMessage(producer *kafka.Producer, msg *kafka.Message) {
	retryCount := dlt.GetRetryCount(msg)
	reason := dlt.GetHeader(msg, dlt.HeaderFailureReason)
	failedAtStr := dlt.GetHeader(msg, dlt.HeaderFailedAt)

	fmt.Printf("🔄 Processing retry #%d for key=%s reason=%s\n", retryCount, string(msg.Key), reason)

	// Avoid negative shift
	shift := retryCount
	if shift < 1 {
		shift = 1
	}

	// ─── Exponential backoff delay ───
	// Calculate delay based on retry count: 5s, 10s, 20s
	delay := dlt.RetryDelayBase * time.Duration(1<<(shift-1))
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}

	// Check if enough time has passed since the failure
	failedAt, _ := strconv.ParseInt(failedAtStr, 10, 64)
	if failedAt > 0 {
		elapsed := time.Since(time.UnixMilli(failedAt))
		if elapsed < delay {
			remaining := delay - elapsed
			fmt.Printf("⏳ Waiting %v before retry (backoff for retry #%d)\n", remaining, retryCount)
			time.Sleep(remaining)
		}
	} else {
		// No timestamp — just apply the full delay
		fmt.Printf("⏳ Applying %v backoff delay for retry #%d\n", delay, retryCount)
		time.Sleep(delay)
	}

	// ─── Check max retries ───
	if retryCount >= dlt.MaxRetries {
		// Exceeded max retries → route to DLT permanently
		fmt.Printf("💀 Max retries (%d) exceeded for key=%s — routing to DLT\n",
			dlt.MaxRetries, string(msg.Key))

		dltTopic := dlt.TopicChatRawDLT
		dltMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &dltTopic,
				Partition: kafka.PartitionAny,
			},
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: msg.Headers,
		}

		if err := producer.Produce(dltMsg, nil); err != nil {
			log.Printf("Failed to route to DLT: %v", err)
		}
		return
	}

	// ─── Re-route back to chat.raw for reprocessing ───
	rawTopic := "chat.raw"
	replayMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &rawTopic,
			Partition: kafka.PartitionAny,
		},
		Key:   msg.Key,
		Value: msg.Value,
		// Note: we carry over retry headers so the chat-processor knows this is a retry
		Headers: msg.Headers,
	}

	if err := producer.Produce(replayMsg, nil); err != nil {
		log.Printf("Failed to re-route to chat.raw: %v", err)
	}

	fmt.Printf("✅ Retry #%d re-routed to chat.raw for key=%s\n", retryCount, string(msg.Key))
}
