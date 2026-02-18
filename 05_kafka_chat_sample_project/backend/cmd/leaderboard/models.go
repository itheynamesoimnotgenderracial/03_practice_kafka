package main

type RoomWindowMetrics struct {
	RoomID      string `bson:"room_id" json:"room_id"`
	WindowStart int64  `bson:"window_start" json:"window_start"`
	// WindowEnd     int64    `bson:"window_end" json:"window_end"`
	TotalMessages int64    `bson:"total_messages" json:"total_messages"`
	ActiveUsers   []string `bson:"active_users" json:"active_users"`
}
