package main

type RoomMetrics struct {
	RoomID        string `bson:"room_id"`
	TotalMessages string `bson:"total_messages"`
	ActiveUsers   string `bson:"active_users"`
	LastUpdated   string `bson:"last_updated"`
}

type RoomWindowMetrics struct {
	RoomID        string          `bson:"room_id"`
	WindowStart   int64           `bson:"window_start"`
	WindowEnd     int64           `bson:"window_end"`
	TotalMessages int64           `bson:"total_messages"`
	ActiveUsers   map[string]bool `bson:"active_users"`
	LastUpdated   int64           `bson:"last_updated"`
}

type ChatTimelineEvent struct {
	MessageID string `avro:"message_id" json:"message_id" bson:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id" bson:"room_id"`
	UserID    string `avro:"user_id" json:"user_id" bson:"user_id"`
	Content   string `avro:"content" json:"content" bson:"content"`
	Sequence  int64  `avro:"sequence" json:"sequence" bson:"sequence"`
	Timestamp int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}

type ChatLeaderboardEvent struct {
	RoomID           string `avro:"room_id" json:"room_id" bson:"room_id"`
	WindowStart      int64  `avro:"window_start" json:"window_start" bson:"window_start"`
	WindowType       string `avro:"window_type" json:"window_type" bson:"window_type"`
	TotalMessages    int64  `avro:"total_messages" json:"total_messages" bson:"total_messages"`
	ActiveUsersCount int64  `avro:"active_users_count" json:"active_users_count" bson:"active_users_count"`
	Timestamp        int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}
