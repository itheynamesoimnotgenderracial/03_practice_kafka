package main

import (
	"context"
	"log"
	"net/http"
	"sample-chat/cmd/api/dltapi"
	"sample-chat/cmd/api/handlers"
	"sample-chat/cmd/api/repository"
	"sample-chat/cmd/api/ws"
	"sample-chat/cmd/utils"
	"sample-chat/internal/auth"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	brokers := utils.GetEnv("KAFKA_BROKERS", "kafka1:29092")
	schemaRegistryURL := utils.GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	redisAddr := utils.GetEnv("REDIS_ADDR", "redis:6379")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("failed to connect to mongoDB:", err)
	}

	collection := client.Database("chat")
	userRepo := auth.NewUserRepository(collection)
	authHandler := handlers.NewAuthHandler(userRepo)
	repo := repository.NewMessageRepository(collection)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		log.Fatal("failed to create kafka producer:", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatal("failed to create schema registry client:", err)
	}

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatal("failed to create avro serializer for leader board:", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	roomManager := ws.NewRoomManager(rdb)

	defer func() {
		var err error

		cancel()
		producer.Close()
		redisClostErr := rdb.Close()
		clientDisconnectErr := client.Disconnect(ctx)
		serializationErr := serializer.Close()

		if clientDisconnectErr != nil {
			err = clientDisconnectErr
		} else if serializationErr != nil {
			err = serializationErr
		} else if redisClostErr != nil {
			err = redisClostErr
		}

		if err != nil {
			log.Fatal("error in closing api service defer errors", err)
		}
	}()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Println("delivery failed:", ev.TopicPartition.Error)
				} else {
					log.Println(
						*ev.TopicPartition.Topic,
						ev.TopicPartition.Partition,
						ev.TopicPartition.Offset,
					)
				}
			}
		}
	}()
	messageHandler := handlers.NewMessageHandler(repo, producer, serializer, ctx)
	dltHandler := dltapi.NewDLTHandler(collection, producer)
	router := gin.Default()
	router.Use(func(ctx *gin.Context) {
		ctx.Header("Access-Control-Allow-Origin", "*")
		ctx.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		ctx.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if ctx.Request.Method == "OPTIONS" {
			ctx.AbortWithStatus(204)
			return
		}
		ctx.Next()
	})

	// Public routes
	router.POST("/api/auth/register", authHandler.Register)
	router.POST("/api/auth/login", authHandler.Login)

	api := router.Group("/api")
	api.Use(auth.AuthMiddleware())
	api.GET("/auth/me", authHandler.Me)
	api.GET("/messages", messageHandler.GetMessages)
	api.POST("/messages", messageHandler.SendMessage)
	dltHandler.RegisterRoutes(api)

	router.GET("/ws/rooms/:roomId", func(ctx *gin.Context) {
		tokenStr := ctx.Query("token")
		if tokenStr == "" {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}

		_, _, err := auth.ValidateToken(tokenStr)
		if err != nil {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		roomID := ctx.Param("roomId")
		if roomID == "" {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "roomId is required"})
			return
		}

		conn, err := wsUpgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Println("Websocket upgrade error:", err)
			return
		}

		hub := roomManager.GetOrCreateHub(roomID)
		ws.ServeWs(hub, conn)
	})

	log.Println("🚀 API server running on :8083")
	log.Println("  REST: /api/messages")
	log.Println("  WS:   /ws/rooms/:roomId")
	router.Run(":8083")
}
