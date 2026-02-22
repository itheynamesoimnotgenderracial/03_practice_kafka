package main

// import (
// 	"context"
// 	"encoding/json"
// 	"log"
// 	"time"

// 	"go.mongodb.org/mongo-driver/bson"
// 	"go.mongodb.org/mongo-driver/mongo"
// 	"go.mongodb.org/mongo-driver/mongo/options"
// )

// type Service interface {
// 	FetchTopRooms(limit int64) ([]RoomStats, error)
// }

// type ServiceStore struct {
// 	Collection *mongo.Collection
// 	Hub        *HubStore
// 	Redis      *RedisClientStore
// }

// func (s *ServiceStore) FetchTopRooms(limit int64) ([]RoomStats, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
// 	defer cancel()

// 	opts := options.Find().
// 		SetSort(bson.D{{Key: "total_messages", Value: -1}}).
// 		SetLimit(limit)

// 	cursor, err := s.Collection.Find(ctx, bson.M{}, opts)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer cursor.Close(ctx)

// 	var results []RoomStats
// 	if err = cursor.All(ctx, &results); err != nil {
// 		return nil, err
// 	}

// 	return results, nil
// }

// func (s *ServiceStore) StartBroadcaster() {
// 	ticker := time.NewTicker(5 * time.Second)

// 	for range ticker.C {
// 		results, err := s.FetchTopRooms(10)
// 		if err != nil {
// 			log.Println("Fetch error:", err)
// 			continue
// 		}

// 		data, err := json.Marshal(results)
// 		if err != nil {
// 			continue
// 		}

// 		err = s.Redis.Publish("leaderboard_updates", data)
// 		if err != nil {
// 			log.Println("Redis publish error:", err)
// 		}
// 	}
// }
