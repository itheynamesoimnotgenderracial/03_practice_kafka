package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"simple-semo-eos/internal/api"
	"simple-semo-eos/internal/consumer"
	"simple-semo-eos/internal/models"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// stubAPI returns a CallerFunc that always succeeds and counts invocations.
func stubAPI(extID string, calls *atomic.Int32) func(models.CustomerMessage) (*models.ExternalAPIResponse, error) {
	return func(msg models.CustomerMessage) (*models.ExternalAPIResponse, error) {
		calls.Add(1)
		return &models.ExternalAPIResponse{ExternalID: extID, Valid: true, Message: "ok"}, nil
	}
}

// startPipeline launches the validator and customer consumer goroutines with
// their own Kafka connections. The pipeline exits when ctx is cancelled.
func startPipeline(
	ctx context.Context,
	t *testing.T,
	groupSuffix string,
	extAPI func(models.CustomerMessage) (*models.ExternalAPIResponse, error),
) {
	t.Helper()
	producer := mustProducer(t)

	go func() {
		consumer.RunValidatorConsumer(ctx, consumer.ValidatorConfig{
			Consumer:        mustConsumer(t, "test-validator-"+groupSuffix),
			Producer:        producer,
			Serde:           testCodec,
			ValidationTopic: testValidationTopic,
			CustomerTopic:   testConsumerTopic,
			DLQTopic:        testDLQTopic,
		})
	}()

	go func() {
		consumer.RunCustomerConsumer(ctx, consumer.CustomerConfig{
			Consumer:      mustConsumer(t, "test-customer-"+groupSuffix),
			Producer:      producer,
			Store:         testStore,
			Serde:         testCodec,
			CustomerTopic: testConsumerTopic,
			DLQTopic:      testDLQTopic,
			ExternalAPI:   extAPI,
		})
	}()
}

func postToRouter(t *testing.T, router http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/customer", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// TestE2E_HappyPath exercises the full pipeline:
//
//	POST /api/customer → validator topic → validator consumer
//	→ consumer topic → customer consumer → Postgres
func TestE2E_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	msgID := "msg-happy-" + shortID()
	custID := "cust-happy-" + shortID()
	var calls atomic.Int32

	startPipeline(ctx, t, shortID(), stubAPI("EXT-HAPPY", &calls))

	apiProducer := mustProducer(t)
	router := api.NewRouter(apiProducer, testCodec, testValidationTopic)

	body := fmt.Sprintf(
		`{"message_id":%q,"customer_id":%q,"name":"Juan","email":"juan@e.com","action":"CREATE"}`,
		msgID, custID,
	)
	rr := postToRouter(t, router, body)
	require.Equal(t, http.StatusAccepted, rr.Code)

	// Poll until the customer row lands in Postgres.
	pollUntil(t, ctx, func() bool {
		c, _ := getCustomerFromDB(ctx, custID)
		return c != nil
	})

	c, err := getCustomerFromDB(ctx, custID)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "Juan", c.Name)
	assert.Equal(t, "EXT-HAPPY", c.ExternalID)
	assert.EqualValues(t, 1, calls.Load(), "external API must be called exactly once")

	key := fmt.Sprintf("%s::%s::CREATE", msgID, custID)
	rec, err := testStore.GetOutboxRecord(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "COMPLETED", rec.Status)
}

// TestE2E_DuplicateMessage_ExtAPICalledOnce publishes the same Kafka message
// twice to simulate at-least-once redelivery. The idempotency check must ensure
// the external API is called exactly once and only one customer row exists.
func TestE2E_DuplicateMessage_ExtAPICalledOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	msgID := "msg-dup-" + shortID()
	custID := "cust-dup-" + shortID()
	var calls atomic.Int32

	startPipeline(ctx, t, shortID(), stubAPI("EXT-DUP", &calls))

	// Produce the same Avro-encoded message to the validation topic twice
	// (same message_id → same idempotency key on second delivery).
	msg := models.CustomerMessage{
		MessageID:  msgID,
		CustomerID: custID,
		Name:       "Dup Test",
		Email:      "dup@e.com",
		Action:     "CREATE",
	}
	payload, err := testCodec.Serialize(testValidationTopic+"-value", msg)
	require.NoError(t, err)

	producer := mustProducer(t)
	require.NoError(t, producer.Publish(testValidationTopic, custID, payload))
	require.NoError(t, producer.Publish(testValidationTopic, custID, payload))

	// Wait for the first delivery to complete.
	pollUntil(t, ctx, func() bool {
		c, _ := getCustomerFromDB(ctx, custID)
		return c != nil
	})

	// Give the second delivery time to be processed (must be a no-op).
	time.Sleep(2 * time.Second)

	assert.EqualValues(t, 1, calls.Load(),
		"external API must be called exactly once even when the same message is delivered twice")
}

// TestE2E_CrashResume_EXT_API_DONE_SkipsExtCall simulates a crash that happened
// after the external API call but before the DB save.  When the consumer sees
// EXT_API_DONE in the outbox it must skip the external API and go straight to
// saving the customer row using the stored response.
func TestE2E_CrashResume_EXT_API_DONE_SkipsExtCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	msgID := "msg-crash-" + shortID()
	custID := "cust-crash-" + shortID()
	key := fmt.Sprintf("%s::%s::CREATE", msgID, custID)
	var calls atomic.Int32

	// Seed an EXT_API_DONE checkpoint — simulates a crash after ext API, before DB save.
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-crash-"+shortID(), key, `{}`))
	require.NoError(t, testStore.UpdateOutboxStatus(ctx, key, "EXT_API_DONE",
		`{"external_id":"EXT-CRASH","valid":true,"message":"ok"}`))

	// Start only the customer consumer — bypass the validator by publishing
	// the redelivered message directly to the consumer topic.
	producer := mustProducer(t)
	go func() {
		consumer.RunCustomerConsumer(ctx, consumer.CustomerConfig{
			Consumer:      mustConsumer(t, "test-crash-resume-"+shortID()),
			Producer:      producer,
			Store:         testStore,
			Serde:         testCodec,
			CustomerTopic: testConsumerTopic,
			DLQTopic:      testDLQTopic,
			ExternalAPI:   stubAPI("SHOULD-NOT-BE-USED", &calls),
		})
	}()

	msg := models.CustomerMessage{
		MessageID:  msgID,
		CustomerID: custID,
		Name:       "Crash Test",
		Email:      "crash@e.com",
		Action:     "CREATE",
	}
	payload, err := testCodec.Serialize(testConsumerTopic+"-value", msg)
	require.NoError(t, err)
	require.NoError(t, producer.Publish(testConsumerTopic, custID, payload))

	// Wait for the outbox to reach COMPLETED.
	pollUntil(t, ctx, func() bool {
		rec, _ := testStore.GetOutboxRecord(ctx, key)
		return rec != nil && rec.Status == "COMPLETED"
	})

	assert.EqualValues(t, 0, calls.Load(),
		"external API must NOT be called when resuming from EXT_API_DONE checkpoint")

	c, err := getCustomerFromDB(ctx, custID)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "EXT-CRASH", c.ExternalID,
		"must use the external_id from the stored EXT_API_DONE checkpoint, not re-call the API")
}
