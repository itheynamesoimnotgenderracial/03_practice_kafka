package main

import (
	"context"
	"fmt"
	"log"
	"sample-chat/cmd/utils"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	router := gin.Default()

	router.GET("/leaderboard/top-rooms", getTopRooms)

	port := "8084"
	router.Run(":" + port)
}

func getTopRooms(c *gin.Context) {
	mongoURI := utils.GetEnv("MONGO_URI", "mongodb://mongo:27017")
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("mongo connection failed:", err)
	}
	collection := client.Database("chat").Collection("room_metrics")
	now := time.Now().UTC()

	windowStart := time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		0, 0, 0,
		time.UTC,
	).UTC().Unix()

	filter := map[string]interface{}{
		"window_start": windowStart,
	}
	fmt.Println("window_start ====>", filter)
	opts := options.Find().
		SetSort(map[string]interface{}{"total_messages": -1}).
		SetLimit(10)

	cursor, err := collection.Find(context.TODO(), filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("cursor ====>", cursor)
	defer func() {
		err = cursor.Close(context.TODO())
		if err != nil {
			c.JSON(500, gin.H{"error with closing context": err.Error()})
			return
		}
	}()

	var results []RoomWindowMetrics
	if err := cursor.All(context.TODO(), &results); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("tresult ====>", results)
	c.JSON(200, results)
}
