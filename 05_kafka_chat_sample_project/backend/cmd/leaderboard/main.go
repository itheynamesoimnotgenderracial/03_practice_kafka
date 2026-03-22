package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sample-chat/cmd/utils"
	"sync"
	"syscall"
	"time"
)

func main() {
	redisAddr := utils.GetEnv("REDIS_ADDR", "localhost:6379")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redis := NewRedisClient(redisAddr)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		StartKafkaConsumer(ctx, redis)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		StartWebsocketServer(ctx, redis)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		StartBroadcaster(ctx, redis)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	log.Println("🚦 Shutdown signal received")

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ Graceful shutdown complete")
	case <-time.After(15 * time.Second):
		log.Println("⚠️ Forced shutdown (timeout)")
	}
}
