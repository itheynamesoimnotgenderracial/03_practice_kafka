package main

import (
	"fmt"
	"sample-chat/cmd/utils"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var p *kafka.Producer
	var err error

	fmt.Println("Processing the api producer...")
	for {
		p, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":  utils.GetEnv("KAFKA_BROKERS", "kafka1:29092"),
			"transactional.id":   "test-producer-1",
			"enable.idempotence": true,
			"acks":               "all",
		})
		if err == nil {
			fmt.Println("Connection established!")
			break
		}

		fmt.Println("Producer init failed, retying...")
		time.Sleep(3 * time.Second)
	}

	defer p.Close()
}
