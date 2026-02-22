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
	// leader := NewLeaderManager(redis)
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// go leader.Start(ctx)
	// go StartWebsocker(redis)

	go StartKafkaConsumer(ctx, redis)
	go StartWebsocketServer(ctx, redis)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	log.Println("Shutting down...")
}
