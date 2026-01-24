package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Point to all brokers in the cluster
	brokers := []string{"localhost:9092", "localhost:9094", "localhost:9096"}
	topic := "acks-demo-topic" // Use the topic we created

	// Select acks level
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose acknowledgment level:")
	fmt.Println("1. acks=0 (RequireNone) - Fire and Forget, fastest, but can lose data.")
	fmt.Println("2. acks=1 (RequireOne) - Leader confirms, balances speed and safety.")
	fmt.Println("3. acks=all (RequireAll) - All in-sync replicas confirm, safest, but slowest.")
	fmt.Print("Enter choice (1/2/3): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var requiredAcks kafka.RequiredAcks
	switch input {
	case "1":
		requiredAcks = kafka.RequireNone
	case "2":
		requiredAcks = kafka.RequireOne
	case "3":
		requiredAcks = kafka.RequireAll
	default:
		fmt.Println("Invalid choice. Using acks=1 by default.")
		requiredAcks = kafka.RequireOne
	}

	// The writer will automatically discover the partition leader.
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: requiredAcks,
		// Add a timeout to see failures more clearly
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	defer writer.Close()

	fmt.Printf("🚀 Starting producer with acks=%v...\n", requiredAcks)
	fmt.Println("You can now stop/start broker containers in another terminal to see the effect.")

	for i := 0; ; i++ {
		msg := fmt.Sprintf("Message with acks=%v, msg #%d", requiredAcks, i)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(msg),
			},
		)
		if err != nil {
			fmt.Printf("❌ FAILED to send message #%d: %v\n", i, err)
		} else {
			fmt.Printf("✅ Message sent: %s\n", msg)
		}
		time.Sleep(2 * time.Second)
	}
}
