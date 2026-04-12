package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"simple-semo-eos/internal/consumer"
	"simple-semo-eos/internal/db"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
	"simple-semo-eos/internal/serde"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func newMsg(msgID, custID, action string) models.CustomerMessage {
	return models.CustomerMessage{
		MessageID:  msgID,
		CustomerID: custID,
		Name:       "Test User",
		Email:      "test@example.com",
		Action:     action,
	}
}

func rawJSON(t *testing.T, msg models.CustomerMessage) string {
	t.Helper()
	b, err := json.Marshal(msg)
	require.NoError(t, err)
	return string(b)
}

func successAPI(extID string) func(models.CustomerMessage) (*models.ExternalAPIResponse, error) {
	return func(cm models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		return &models.ExternalAPIResponse{
			ExternalID: extID,
			Valid:      true,
			Message:    "ok",
		}, nil
	}
}

func failThenSucceed(failN int, extID string) func(models.CustomerMessage) (*models.ExternalAPIResponse, error) {
	calls := 0
	return func(cm models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		calls++
		if calls <= failN {
			return nil, fmt.Errorf("transient call %d)", calls)
		}
		return &models.ExternalAPIResponse{
			ExternalID: extID,
			Valid:      true,
		}, nil
	}
}

func newCfg(store db.Store, producer kafka.Producer, api func(models.CustomerMessage) (*models.ExternalAPIResponse, error)) consumer.CustomerConfig {
	return consumer.CustomerConfig{
		Consumer:      kafka.NewMockConsumer(),
		Producer:      producer,
		Store:         store,
		Serde:         serde.JSONCodec{},
		CustomerTopic: "simple.eos.consumer",
		DLQTopic:      "simple.eos.dlq",
		ExternalAPI:   api,
	}
}

// ── Happy path ────────────────────────────────────────────────────────────────

func TestProcess_HappyPath_CompleteSuccessfully(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-happy", "cust-happy", "CREATE")

	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, successAPI("EXT-001")), msg, rawJSON(t, msg))
	require.NoError(t, err)

	key := consumer.BuildIdempotencyKey(msg)
	rec, _ := store.GetOutboxRecord(ctx, key)
	require.NotNil(t, rec)
	assert.Equal(t, "COMPLETED", rec.Status)

	c := store.GetCustomer("cust-happy")
	require.NotNil(t, c)
	assert.Equal(t, "EXT-001", c.ExternalID)
	assert.Empty(t, producer.PublishedTo("simple.eos.dlq"), "no DLQ messages on happy path")
}

// ── Duplicate detection ────────────────────────────────────────────────────────

func TestProcess_DuplicateDelivery_ExternalAPICalledOnlyOnce(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-dup", "cust-dup", "CREATE")
	raw := rawJSON(t, msg)
	calls := 0
	api := func(m models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		calls++
		return &models.ExternalAPIResponse{
			ExternalID: "EXT-DUP",
			Valid:      true,
		}, nil
	}
	cfg := newCfg(store, producer, api)
	require.NoError(t, consumer.ProcessWithIdempotencyForTest(ctx, cfg, msg, raw))
	require.NoError(t, consumer.ProcessWithIdempotencyForTest(ctx, cfg, msg, raw))

	assert.Equal(t, 1, calls, "external API must be called exactly once")
	key := consumer.BuildIdempotencyKey(msg)
	rec, _ := store.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)
}
