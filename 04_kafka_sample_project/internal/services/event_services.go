package services

import (
	"context"
	"errors"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"time"

	"github.com/google/uuid"
)

type EventService struct {
	producer *kafka.Producer
}

func NewEventService(producer *kafka.Producer) *EventService {
	return &EventService{producer: producer}
}

func (s *EventService) IngestEvent(ctx context.Context, event *models.Event) error {
	if event.EventType == "" {
		return errors.New("event_type is required")
	}

	event.EventID = uuid.NewString()
	event.Timestamp = time.Now().UTC()

	return s.producer.Publish(ctx, event.EventType, event)
}
