package pkgmodels

type ChatMessage struct {
	MessageID string `avro:"message_id" json:"message_id" bson:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id" bson:"room_id"`
	UserID    string `avro:"user_id" json:"user_id" bson:"user_id"`
	Content   string `avro:"content" json:"content" bson:"content"`
	Sequence  int64  `avro:"sequence" json:"sequence" bson:"sequence"`
	Timestamp int64  `avro:"timestamp" json:"timestamp" bson:"timestamp"`
}

type ChatRawEvent struct {
	MessageID string `avro:"message_id" bson:"message_id" json:"message_id"`
	RoomID    string `avro:"room_id" bson:"room_id" json:"room_id"`
	UserID    string `avro:"user_id" bson:"user_id" json:"user_id"`
	Content   string `avro:"content" bson:"content" json:"content"`
	Timestamp int64  `avro:"timestamp" bson:"timestamp" json:"timestamp"`
}
