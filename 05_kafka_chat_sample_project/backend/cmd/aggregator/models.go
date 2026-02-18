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
