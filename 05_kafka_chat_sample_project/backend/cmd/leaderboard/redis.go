package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Publish(channel string, message []byte) error
	Subscribe(channel string, handler func([]byte))
	TryAcquireLock(key string, ttl time.Duration) (bool, error)
	RenewLock(key string, ttl time.Duration) error
}

type RedisClientStore struct {
	Client     *redis.Client
	instanceID string
	RedisClient
}

func NewRedisClient(redisAddr string) *RedisClientStore {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &RedisClientStore{
		Client:     rdb,
		instanceID: generateInstanceID(),
	}
}

func (r *RedisClientStore) Publish(ctx context.Context, channel string, message []byte) error {
	return r.Client.Publish(ctx, channel, message).Err()
}

func (r *RedisClientStore) Subscribe(ctx context.Context, channel string, handler func([]byte)) {
	pubsub := r.Client.Subscribe(ctx, channel)

	go func() {
		ch := pubsub.Channel()
		for msg := range ch {
			handler([]byte(msg.Payload))
		}
	}()
}

func (r *RedisClientStore) TryAcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.Client.SetNX(ctx, key, r.instanceID, ttl).Result()
}

func (r *RedisClientStore) RenewLock(ctx context.Context, key string, ttl time.Duration) error {
	val, err := r.Client.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	if val != r.instanceID {
		return redis.Nil
	}

	return r.Client.Expire(ctx, key, ttl).Err()
}

func (r *RedisClientStore) UpdateScore(ctx context.Context, roomID string, score int64) error {
	return r.Client.ZIncrBy(ctx, "leaderboard", float64(score), roomID).Err()
}

func (r *RedisClientStore) GetTopN(ctx context.Context, n int64) ([]LeaderboardEntry, error) {
	results, err := r.Client.ZRevRangeWithScores(ctx, "leaderboard", 0, n-1).Result()
	if err != nil {
		return nil, err
	}

	leaderboard := make([]LeaderboardEntry, len(results))
	for _, z := range results {
		leaderboard = append(leaderboard, LeaderboardEntry{
			RoomID:        z.Member.(string),
			TotalMessages: int64(z.Score),
		})
	}

	return leaderboard, nil
}

func (r *RedisClientStore) PublishLeaderboard(ctx context.Context, payload []byte) error {
	return r.Client.Publish(ctx, "leaderboard_updates", payload).Err()
}

func generateInstanceID() string {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}
