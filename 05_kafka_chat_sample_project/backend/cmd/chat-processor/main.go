package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sample-chat/cmd/utils"
	"sample-chat/internal/handler"
	"sample-chat/internal/kafka"

	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

var (
	groupID       = "chat-processor-group"
	transactionID = "chat-processor-1"
)

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	chatRawDeserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatal("failed to create avro deserializer:", err)
	}

	chatValidatedSerializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("failed to create avro serializer:", err)
	}

	consumer, err := newConsumer(brokers, groupID)
	if err != nil {
		log.Fatal("setup newConsumer failed:", err)
	}

	producerCfg := kafka.NewProducerConfig(brokers, transactionID)
	txProducer, err := kafka.NewTxProducer(producerCfg)
	if err != nil {
		log.Fatal("setup newConsumer failed:", err)
	}

	defer func() {
		var err error
		consumerErr := consumer.Close()
		txProducerErr := txProducer.Abort(ctx)
		txCommitErr := txProducer.Commit(ctx)
		chatRawDeserializerErr := chatRawDeserializer.Close()
		chatValidatedSerializerErr := chatValidatedSerializer.Close()
		if consumerErr != nil {
			err = consumerErr
		} else if txProducerErr != nil {
			err = txProducerErr
		} else if txCommitErr != nil {
			err = txCommitErr
		} else if chatRawDeserializerErr != nil {
			err = chatRawDeserializerErr
		} else if chatValidatedSerializerErr != nil {
			err = chatValidatedSerializerErr
		}

		if err != nil {
			log.Fatal("error in closing defer consumer", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Println("Received signal:", sig)
		cancel()
	}()

	validationHandler := handler.ChatValidationHandler(chatRawDeserializer, chatValidatedSerializer)

	err = kafka.RunProcessor(ctx, consumer, txProducer.(*kafka.TxProducer), validationHandler)
	if err != nil {
		log.Println("processor exited:", err)
	}

	log.Println("service stopped cleanly")
}

func newConsumer(brokers, groupID string) (*baseKafka.Consumer, error) {
	consumer, err := baseKafka.NewConsumer(&baseKafka.ConfigMap{
		"bootstrap.servers":               brokers,
		"group.id":                        groupID,
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              false,
		"isolation.level":                 "read_committed",
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		return nil, err
	}

	err = consumer.SubscribeTopics(
		[]string{"chat.raw"},
		func(c *baseKafka.Consumer, e baseKafka.Event) error {
			switch ev := e.(type) {
			case baseKafka.AssignedPartitions:
				log.Println("Partitions assigned:", ev.Partitions)
				return c.Assign(ev.Partitions)
			case baseKafka.RevokedPartitions:
				log.Println("Partitions revoked:", ev.Partitions)
				return c.Unassign()
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}
