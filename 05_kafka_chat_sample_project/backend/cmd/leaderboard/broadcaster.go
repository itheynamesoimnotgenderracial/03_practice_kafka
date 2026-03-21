package main

import (
	"context"
	"log"
	"time"
)

func StartBroadcaster(ctx context.Context, redis *RedisClientStore) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Broadcaster stopped")
			return
		case <-ticker.C:
			redis.Client.Publish(ctx, "leaderboard_updates:hourly", "refresh")
			redis.Client.Publish(ctx, "leaderboard_updates:daily", "refresh")
		}
	}
}
