package main

import "time"

type RoomWindowMetrics struct {
	RoomID      string `bson:"room_id" json:"room_id"`
	WindowStart int64  `bson:"window_start" json:"window_start"`
	// WindowEnd     int64    `bson:"window_end" json:"window_end"`
	TotalMessages int64    `bson:"total_messages" json:"total_messages"`
	ActiveUsers   []string `bson:"active_users" json:"active_users"`
}

type RoomStats struct {
	RoomID        string    `bson:"room_id" json:"room_id"`
	TotalMessages int64     `bson:"total_messages" json:"total_messages"`
	LastMessage   string    `bson:"last_message" json:"last_message"`
	LastUpdated   time.Time `bson:"last_updated" json:"last_updated"`
}

type LeaderboardEntry struct {
	RoomID        string `json:"room_id"`
	TotalMessages int64  `json:"total_messages"`
}

type ChatLeaderboardEvent struct {
	RoomID           string `avro:"room_id" json:"room_id" bson:"room_id"`
	WindowStart      int64  `avro:"window_start" json:"window_start" bson:"window_start"`
	WindowType       string `avro:"window_type" json:"window_type" bson:"window_type"`
	TotalMessages    int64  `avro:"total_messages" json:"total_messages" bson:"total_messages"`
	ActiveUsersCount int64  `avro:"active_users_count" json:"active_users_count" bson:"active_users_count"`
	Timestamp        int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}
