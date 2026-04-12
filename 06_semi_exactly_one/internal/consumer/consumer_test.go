package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
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

// ── Crash recovery: EXT_API_DONE ─────────────────────────────────────────────

func TestProcess_ResumesFromExtAPIDone_SkipExternalAPI(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-resume", "cust-resume", "RESUME")
	key := consumer.BuildIdempotencyKeyForTest(msg)

	// Simulate: process crashed after ext API but before DB save
	store.Seed(db.OutboxRecord{
		ID:             "id-crash",
		IdempotencyKey: key,
		Status:         "EXT_API_DONE",
		ExAPIResponse:  `{"external_id":"EXT-RESUME","valid":true,"message":"ok"}`,
	})

	calls := 0
	api := func(_ models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		calls++
		return &models.ExternalAPIResponse{ExternalID: "SHOULD-NOT-HAPPEN"}, nil
	}

	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, api), msg, rawJSON(t, msg))
	require.NoError(t, err)
	assert.Equal(t, 0, calls, "ext API must NOT be called when resuming from EXT_API_DONE")
	rec, _ := store.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)

	c := store.GetCustomer("cust-resume")
	require.NotNil(t, c)
	assert.Equal(t, "EXT-RESUME", c.ExternalID, "must use stored ext response, not re-call the API")
}

// ── Crash recovery: PENDING ───────────────────────────────────────────────────

func TestProcess_ResumesFromPending_CallsExternalAPI(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-pend", "cst-pend", "CREATE")
	key := consumer.BuildIdempotencyKeyForTest(msg)

	// Simulate: process crashed right after writing PENDING, before ext API
	store.Seed(db.OutboxRecord{ID: "id-pend", IdempotencyKey: key, Status: "PENDING"})

	calls := 0
	api := func(_ models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		calls++
		return &models.ExternalAPIResponse{ExternalID: "EXT-PEND", Valid: true}, nil
	}

	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, api), msg, rawJSON(t, msg))
	require.NoError(t, err)

	assert.Equal(t, 1, calls, "ext API must be called once when resuming from PENDING")
	rec, _ := store.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)
}

// ── DLQ replay: FAILED ────────────────────────────────────────────────────────

func TestProcess_FailedStatus_ResetsAndCompletesSuccessfully(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-fail", "cust-fail", "CREATE")
	key := consumer.BuildIdempotencyKeyForTest(msg)

	store.Seed(db.OutboxRecord{
		ID:             "id-fail",
		IdempotencyKey: key,
		Status:         "FAILED",
		FailedAtStep:   "EXTERNAL_API",
		FailureReason:  "previous timeout",
	})

	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, successAPI("EXT-REPLAY")), msg, rawJSON(t, msg))
	require.NoError(t, err)

	rec, _ := store.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)
}

// ── External API failures ─────────────────────────────────────────────────────

func TestProcess_ExtAPITransientFailure_RetriesAndSucceeds(t *testing.T) {
	store := db.NewMockStore()
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-transient", "cust-transient", "CREATE")

	// Fail 2 times, succeed on 3rd (maxRetries = 3)
	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, failThenSucceed(2, "EXT-RETRY")), msg, rawJSON(t, msg))
	require.NoError(t, err)

	key := consumer.BuildIdempotencyKeyForTest(msg)
	rec, _ := store.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)
	assert.Empty(t, producer.PublishedTo("simple.eos.dlq"))
}

// ── DB save failure ───────────────────────────────────────────────────────────

func TestProcess_DBSaveFails_SendDLQ(t *testing.T) {
	store := db.NewMockStore()
	store.ForceSaveError = errors.New("postgres: connection reset")
	producer := kafka.NewMockProducer()
	msg := newMsg("msg-dbfail", "cust-dbfail", "CREATE")

	err := consumer.ProcessWithIdempotencyForTest(ctx, newCfg(store, producer, successAPI("EXT-1")), msg, rawJSON(t, msg))
	require.NoError(t, err)

	dlqMsgs := producer.PublishedTo("simple.eos.dlq")
	fmt.Println(string(dlqMsgs[0].Value))
	require.Len(t, dlqMsgs, 1)
	var dlq models.DLQMessage
	require.NoError(t, json.Unmarshal(dlqMsgs[0].Value, &dlq))
	fmt.Println(dlq)
	assert.Equal(t, "DB_SAVE", dlq.FailedAtStep)
}

// ── ValidateMessage ───────────────────────────────────────────────────────────

func TestValidateMessage_ValidateCreateMessage_NoError(t *testing.T) {
	msg := models.CustomerMessage{CustomerID: "c1", Name: "Juan", Email: "juan@example.com", Action: "CREATE"}
	assert.NoError(t, consumer.ValidateMessageForTest(msg))
}

func TestValidateMessage_ValidUpdateMessage_NoError(t *testing.T) {
	msg := models.CustomerMessage{CustomerID: "c1", Name: "Juan", Email: "juan@example.com", Action: "UPDATE"}
	assert.NoError(t, consumer.ValidateMessageForTest(msg))
}

func TestValidateMessage_MissingFields_ReturnsErrors(t *testing.T) {
	cases := []struct {
		desc string
		msg  models.CustomerMessage
	}{
		{
			"missing customer_id",
			models.CustomerMessage{
				Name:   "J",
				Email:  "j@e.com",
				Action: "CREATE",
			},
		},
		{
			"missing name",
			models.CustomerMessage{
				CustomerID: "c1",
				Email:      "j@e.com",
				Action:     "CREATE",
			},
		},
		{
			"missing email",
			models.CustomerMessage{
				CustomerID: "c1",
				Name:       "J",
				Action:     "CREATE",
			},
		},
		{
			"invalid action DELETE",
			models.CustomerMessage{
				CustomerID: "c1",
				Name:       "J",
				Email:      "j@e.com",
				Action:     "DELETE",
			},
		},
		{
			"empty action",
			models.CustomerMessage{
				CustomerID: "c1",
				Name:       "J",
				Email:      "j@e.com",
				Action:     "",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Error(t, consumer.ValidateMessageForTest(tc.msg))
		})
	}
}
