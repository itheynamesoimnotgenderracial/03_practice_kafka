package main

import (
	"context"
	"log"
	"sample-chat/cmd/api/handlers"
	"sample-chat/cmd/api/repository"
	"sample-chat/cmd/utils"
	"sample-chat/internal/kafka"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	transactionID = "chat-raw-1"
)

func main() {
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	brokers := utils.GetEnv("KAFA_BROKERS", "kafka1:29092")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to mongoDB:", err)
	}

	collection := client.Database("chat")
	repo := repository.NewMessageRepository(collection)
	producerCfg := kafka.NewProducerConfig(brokers, transactionID)
	txProducer, err := kafka.NewTxProducer(producerCfg)
	if err != nil {
		log.Fatal("failed to create a transaction producer in api:", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("failed to create avro serializer for leader board:", err)
	}

	defer func() {
		var err error

		cancel()
		txProducerErr := txProducer.Abort(ctx)
		txCommitErr := txProducer.Commit(ctx)

		if txProducerErr != nil {
			err = txProducerErr
		} else {
			err = txCommitErr
		}

		if err != nil {
			log.Fatal("error in closing defer txProducer", err)
		}
	}()
	messageHandler := handlers.NewMessageHandler(repo, txProducer, serializer, ctx)

	router := gin.Default()
	api := router.Group("/api")
	api.GET("/messages", messageHandler.GetMessages)
	api.POST("/messages", messageHandler.SendMessage)
	router.Run(":8083")
}
