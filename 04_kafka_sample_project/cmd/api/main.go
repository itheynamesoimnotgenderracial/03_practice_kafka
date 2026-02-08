package main

import (
	"events_analytics_platform/cmd/util"
	"events_analytics_platform/internal/handler"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/services"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gin-gonic/gin"
)

func main() {
	// brokers := []string{"kafka1:29092", "kafka2:29092", "kafka3:29092"}
	brokers := strings.Split(util.GetEnv("KAFKA_BROKERS", "kafka1:29092"), ",")
	topic := util.GetEnv("KAFKA_TOPIC", "raw-user-events")

	producer := kafka.NewProducer(brokers, topic)
	defer func() {
		err := producer.Close()
		if err != nil {
			log.Fatal("error when closing raw producer:", err)
		}
	}()

	eventService := services.NewEventService(producer)
	eventHandler := handler.NewEventHandler(eventService)

	router := gin.Default()
	router.POST("/events", eventHandler.PostEvent)

	go func() {
		log.Println("API listening on: 8083")
		if err := router.Run(":8083"); err != nil {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down API")
}
