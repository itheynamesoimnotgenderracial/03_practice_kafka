package models

import "time"

func ConvertModelToNormalizedOrder(event *AuditEvent) NormalizedOrderEvent {
	return NormalizedOrderEvent{
		EventID:     event.EventID,
		UserID:      event.UserID,
		EventType:   event.EventType,
		Source:      event.Source,
		OrderID:     int64(event.Payload["order_id"].(float64)),
		Amount:      event.Payload["amount"].(float64),
		ReceivedAt:  event.ReceivedAt,
		ProcessedAt: time.Now().UTC(),
	}
}
