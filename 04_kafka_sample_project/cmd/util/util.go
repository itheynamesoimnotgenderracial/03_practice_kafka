package util

import (
	"context"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"fmt"
	"os"
	"time"
)

func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func SendDLQ(
	ctx context.Context,
	producer *kafka.Producer,
	key string,
	dqlOriginalError error,
	payload any,
) error {
	eventError := models.EventError{
		Error:         fmt.Sprintf("invalid json, sending to DLQ: %v", dqlOriginalError),
		OriginalEvent: payload,
		FailedAt:      time.Now().UTC(),
	}

	dqlEventError := producer.Publish(ctx, key, eventError)
	if dqlEventError != nil {
		return dqlEventError
	}

	return dqlOriginalError
}
