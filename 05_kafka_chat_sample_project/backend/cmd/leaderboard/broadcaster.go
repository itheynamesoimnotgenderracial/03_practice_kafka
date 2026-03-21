package main

import (
	"context"
	"encoding/json"
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
			// redis.Client.Publish(ctx, "leaderboard_updates:hourly", "refresh")
			// redis.Client.Publish(ctx, "leaderboard_updates:daily", "refresh")

			for _, windowType := range []string{"hourly", "daily"} {
				top, err := redis.GetTopN(ctx, 10, windowType)
				if err != nil {
					log.Println("Broadcaster fetch error:", err)
					continue
				}
				if len(top) == 0 {
					continue
				}

				payload, err := json.Marshal(top)
				if err != nil {
					log.Println("Broadcaster marshal error:", err)
					continue
				}

				err = redis.PublishLeaderboard(ctx, payload, windowType)
				if err != nil {
					log.Println("Broadcaster publish error:", err)
				}
			}
		}
	}
}
