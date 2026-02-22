package main

import (
	"context"
	"log"
	"time"
)

type LeaderManager interface {
	Start(ctx context.Context)
}

type LeaderManagerStore struct {
	redis      *RedisClientStore
	cancelFunc context.CancelFunc
	isLeader   bool
}

func NewLeaderManager(redis *RedisClientStore) *LeaderManagerStore {
	return &LeaderManagerStore{redis: redis}
}

func (l *LeaderManagerStore) Start(ctx context.Context) {
	lockKey := "leaderboard_lock"
	ttl := 10 * time.Second

	for {
		select {
		case <-ctx.Done():
			if l.cancelFunc != nil {
				l.cancelFunc()
			}
			return
		default:
		}

		acquired, err := l.redis.TryAcquireLock(ctx, lockKey, ttl)
		if err == nil && acquired && !l.isLeader {
			log.Println("🔥 Became Leader")

			leaderCtx, cancel := context.WithCancel(context.Background())
			l.cancelFunc = cancel
			l.isLeader = true

			go StartBroadcaster(leaderCtx, l.redis)
		}
	}
}
