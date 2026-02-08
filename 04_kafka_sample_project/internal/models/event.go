package models

import "time"

type Event struct {
	EventID   string                 `json:"event_id"`
	EventType string                 `json:"event_type"`
	Source    string                 `json:"source"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
	UserID    string                 `json:"user_id"`
}

type AuditEvent struct {
	ID         string                 `bson:"_id,omitempty" json:"_id,omitempty"`
	EventID    string                 `bson:"event_id" json:"event_id"`
	UserID     string                 `bson:"user_id" json:"user_id"`
	EventType  string                 `bson:"event_type" json:"event_type"`
	Source     string                 `bson:"source" json:"source"`
	Payload    map[string]interface{} `bson:"payload" json:"payload"`
	ReceivedAt time.Time              `bson:"received_at" json:"received_at"`
}

type EventError struct {
	Error         string    `bson:"error" json:"error"`
	OriginalEvent any       `bson:"original_event" json:"original_event"`
	FailedAt      time.Time `bson:"failed_at" json:"failed_at"`
}

type NormalizedOrderEvent struct {
	EventID     string    `bson:"event_id" json:"event_id"`
	UserID      string    `bson:"user_id" json:"user_id"`
	EventType   string    `bson:"event_type" json:"event_type"`
	Source      string    `bson:"source" json:"source"`
	OrderID     int64     `bson:"order_id" json:"order_id"`
	Amount      float64   `json:"amount" bson:"amount"`
	ReceivedAt  time.Time `bson:"received_at" json:"received_at"`
	ProcessedAt time.Time `json:"processed_at" bson:"processed_at"`
}

type UserOrderAggregates struct {
	UserID      string    `bson:"user_id" json:"user_id"`
	TotalOrders int64     `bson:"total_orders" json:"total_orders"`
	TotalAmount float64   `bson:"total_amount" json:"total_amount"`
	LastOrderAt time.Time `bson:"last_order_at" json:"last_order_at"`
	UpdatedAt   time.Time `bson:"updated_at" json:"updated_at"`
}

type UserTotalOrder struct {
	Id          string `bson:"id" json:"id"`
	TotalAmount int64  `bson:"total_amount" json:"total_amount"`
	TotalOrders int64  `bson:"total_orders" json:"total_orders"`
}

type UserOrderWindowAggregate struct {
	UserID      string    `bson:"user_id" json:"user_id"`
	WindowType  string    `bson:"window_type" json:"window_type"`
	WindowStart time.Time `bson:"window_start" json:"window_start"`
	TotalOrders int64     `bson:"total_orders" json:"total_orders"`
	TotalAmount float64   `bson:"total_amount" json:"total_amount"`
	LastOrderAt time.Time `bson:"last_order_at" json:"last_order_at"`
	UpdatedAt   time.Time `bson:"updated_at" json:"updated_at"`
}
