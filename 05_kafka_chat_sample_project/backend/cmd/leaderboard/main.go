package main

import (
	"context"
	"log"
	"sample-chat/cmd/utils"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")))
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("chat").Collection("room_metrics")

	hub := NewHub()
	go hub.Run()

	service := &ServiceStore{
		Collection: collection,
		Hub:        hub,
	}

	go service.StartBroadcaster()

	router := gin.Default()
	router.GET("/leaderboard/top-rooms", func(ctx *gin.Context) {
		results, err := service.FetchTopRooms(10)
		if err != nil {
			ctx.JSON(500, gin.H{"error": "Failed"})
			return
		}
		ctx.JSON(200, results)
	})

	router.GET("/ws/leaderboard", HandleWebSocket(hub))
	port := "8084"
	router.Run(":" + port)
}
