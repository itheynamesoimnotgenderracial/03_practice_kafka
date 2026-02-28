package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"syscall"
)

func main() {
	redisAddr := utils.GetEnv("REDIS_ADDR", "localhost:6379")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redis := NewRedisClient(redisAddr)

	go StartKafkaConsumer(ctx, redis)
	go StartWebsocketServer(ctx, redis)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	log.Println("Shutting down...")
}
