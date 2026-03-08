package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"sample-chat/internal/handler"
	"sample-chat/internal/kafka"
	"sample-chat/internal/mongo"
	"syscall"
	"time"

	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	baseMongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	groupID       = "timeline-processor-group"
	transactionID = "timeline-processor-1"
)

func main() {
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mongoCtx, mongoCancel := context.WithTimeout(context.Background(), 10*time.Second)
	mongoClient, err := baseMongo.Connect(mongoCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to MongoDB:", err)
	}

	defer func() {
		cancel()
		mongoCancel()
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Println("mongo disconnect error:", err)
		}
	}()

	if err := mongoClient.Ping(mongoCtx, nil); err != nil {
		log.Fatal("mongo disconnect error:", err)
	}
	log.Println("✅ Connected to MongoDB")

	db := mongoClient.Database("chat")
	seqStore := mongo.NewSequenceStore(db, "room_sequences")

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	validatedDeserializer, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatal("failed to create avro deserializer:", err)
	}
	defer func() {
		err = validatedDeserializer.Close()
		if err != nil {
			log.Println("failed on closing validatedDeserializer:", err)
		}
	}()

	timelineSerializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("failed to create avro serializer:", err)
	}
	defer func() {
		err = timelineSerializer.Close()
		if err != nil {
			log.Println("failed on closing timelineSerializer:", err)
		}
	}()

	consumer, err := newConsumer(brokers, groupID)
	if err != nil {
		log.Fatal("setup newConsumer failed:", err)
	}

	producerCfg := kafka.NewProducerConfig(brokers, transactionID)
	txProducer, err := kafka.NewTxProducer(producerCfg)
	if err != nil {
		log.Fatal("setup txProducer failed:", err)
	}

	defer func() {
		var closeErr error
		consumerErr := consumer.Close()
		txProducerErr := txProducer.Abort(ctx)
		txCommitErr := txProducer.Commit(ctx)
		if consumerErr != nil {
			closeErr = consumerErr
		} else if txProducerErr != nil {
			closeErr = txProducerErr
		} else if txCommitErr != nil {
			closeErr = txCommitErr
		}

		if closeErr != nil {
			log.Println("error in closing defer:", closeErr)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Println("Received signal:", sig)
		cancel()
	}()

	timelineHandler := handler.NewTimelineHandler(validatedDeserializer, timelineSerializer, seqStore)
	log.Println("🚀 Timeline processor running — consuming chat.validated → producing chat.timeline")

	err = kafka.RunProcessor(ctx, consumer, txProducer.(*kafka.TxProducer), timelineHandler)
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
		[]string{"chat.validated"},
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
