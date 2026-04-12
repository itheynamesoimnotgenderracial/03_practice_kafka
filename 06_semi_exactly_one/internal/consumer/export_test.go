package consumer

import (
	"context"
	"simple-semo-eos/internal/models"
)

func BuildIdempotencyKeyForTest(msg models.CustomerMessage) string {
	return BuildIdempotencyKey(msg)
}

func ProcessWithIdempotencyForTest(ctx context.Context, cfg CustomerConfig, msg models.CustomerMessage, raw string) error {
	return processWithIdempotency(ctx, cfg, msg, raw)
}

func ValidateMessageForTest(msg models.CustomerMessage) error {
	return ValidateMessage(msg)
}
