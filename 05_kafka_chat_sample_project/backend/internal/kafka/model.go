package kafka

type ChatLeaderboardEvent struct {
	RoomID           string `avro:"room_id" json:"room_id" bson:"room_id"`
	WindowStart      int64  `avro:"window_start" json:"window_start" bson:"window_start"`
	WindowType       string `avro:"window_type" json:"window_type" bson:"window_type"`
	TotalMessages    int64  `avro:"total_messages" json:"total_messages" bson:"total_messages"`
	ActiveUsersCount int64  `avro:"active_users_count" json:"active_users_count" bson:"active_users_count"`
	Timestamp        int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}
