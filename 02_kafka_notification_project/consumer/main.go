package main

import (
	"consumer/pkg/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

const (
	ConsumerGroup       = "notifications-group"
	ConsumerTopic       = "notifications"
	ConsumerPort        = ":8082"
	Kafka1ServerAddress = "localhost:9092"
	Kafka2ServerAddress = "localhost:9094"
	Kafka3ServerAddress = "localhost:9095"
)

var ErrNoMessageFound = errors.New("no message found")

func getUserIDFromRequest(ctx *gin.Context) (string, error) {
	userID := ctx.Param("userID")
	if userID == "" {
		return "", ErrNoMessageFound
	}
	return userID, nil
}

type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userID string, notification models.Notification) {
	newNotifications := make([]models.Notification, len(ns.data[userID])+1)
	ns.mu.Lock()
	defer ns.mu.Unlock()
	copy(newNotifications, ns.data[userID])
	newNotifications = append(newNotifications, notification)
	ns.data[userID] = newNotifications
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userID]
}

type Consumer struct {
	store *NotificationStore
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{Kafka1ServerAddress, Kafka2ServerAddress, Kafka3ServerAddress},
		Topic:   ConsumerTopic,
		GroupID: ConsumerGroup,
	})
	defer consumer.Close()

	consumerStore := &Consumer{
		store: store,
	}

	for {
		msg, err := consumer.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("consumer context canceled, shutting down")
				return
			}
			log.Printf("consumer read error (retrying): %v\n", err)
			time.Sleep(time.Second)
			continue // 👈 DO NOT return
		}

		userID := string(msg.Key)
		var notification models.Notification
		if err := json.Unmarshal(msg.Value, &notification); err != nil {
			log.Printf("failed to unmarshal notification: %v\n", err)
			continue
		}
		consumerStore.store.Add(userID, notification)
	}
}

func handleNotifications(ctx *gin.Context, store *NotificationStore) {
	userID, err := getUserIDFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
		return
	}

	notes := store.Get(userID)
	if len(notes) == 0 {
		ctx.JSON(http.StatusOK, gin.H{
			"message":       "No notifications found for user",
			"notifications": []models.Notification{},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"notifications": notes})
}

func main() {

	store := &NotificationStore{
		data: make(UserNotifications),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/notifications/:userID", func(ctx *gin.Context) {
		handleNotifications(ctx, store)
	})

	go func() {
		fmt.Printf("Kafka CONSUMER (Group: %s) 👥📥 "+"started at http://localhost%s\n", ConsumerGroup, ConsumerPort)

		if err := router.Run(ConsumerPort); err != nil {
			log.Printf("failed to run the server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down consumer...")
	cancel()
}
