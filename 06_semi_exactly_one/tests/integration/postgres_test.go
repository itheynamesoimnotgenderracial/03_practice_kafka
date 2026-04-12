package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgres_OutboxLifecycle tests the full PENDING → EXT_API_DONE → COMPLETED
// progression against the real Postgres schema.
func TestPostgres_OutboxLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	key := "lifecycle-key-" + shortID()
	id := "lifecycle-id-" + shortID()
	custID := "lifecycle-cust-" + shortID()

	// PENDING
	require.NoError(t, testStore.CreateOutboxRecord(ctx, id, key, `{"customer_id":"c1"}`))
	rec, err := testStore.GetOutboxRecord(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "PENDING", rec.Status)
	assert.Equal(t, key, rec.IdempotencyKey)

	// EXT_API_DONE
	extResp := `{"external_id":"EXT-001","valid":true}`
	require.NoError(t, testStore.UpdateOutboxStatus(ctx, key, "EXT_API_DONE", extResp))
	rec, _ = testStore.GetOutboxRecord(ctx, key)
	assert.Equal(t, "EXT_API_DONE", rec.Status)
	assert.Equal(t, extResp, rec.ExAPIResponse)

	// COMPLETED — atomic: upserts customer + marks outbox done in one transaction
	require.NoError(t, testStore.SaveCustomerAndCompleteOutbox(ctx, custID, "Juan", "juan@e.com", "EXT-001", key))
	rec, _ = testStore.GetOutboxRecord(ctx, key)
	assert.Equal(t, "COMPLETED", rec.Status)

	c, err := getCustomerFromDB(ctx, custID)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "Juan", c.Name)
	assert.Equal(t, "EXT-001", c.ExternalID)
}

func TestPostgres_GetOutboxRecord_NotFound_ReturnsNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rec, err := testStore.GetOutboxRecord(ctx, "ghost-key-"+shortID())
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestPostgres_DuplicateIdempotencyKey_IsRejectedByDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	key := "dup-key-" + shortID()
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-1-"+shortID(), key, `{}`))

	err := testStore.CreateOutboxRecord(ctx, "id-2-"+shortID(), key, `{}`)
	assert.Error(t, err, "UNIQUE constraint on idempotency_key must reject a second insert")
}

func TestPostgres_MarkOutboxFailed_SetsAllFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	key := "fail-key-" + shortID()
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-f-"+shortID(), key, `{}`))
	require.NoError(t, testStore.MarkOutboxFailed(ctx, key, "EXTERNAL_API", "connection timeout"))

	rec, err := testStore.GetOutboxRecord(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "FAILED", rec.Status)
	assert.Equal(t, "EXTERNAL_API", rec.FailedAtStep)
	assert.Equal(t, "connection timeout", rec.FailureReason)
	assert.Equal(t, 1, rec.RetryCount)
}

func TestPostgres_IncrementRetryCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	key := "retry-key-" + shortID()
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-r-"+shortID(), key, `{}`))

	require.NoError(t, testStore.IncrementRetryCount(ctx, key))
	require.NoError(t, testStore.IncrementRetryCount(ctx, key))

	rec, err := testStore.GetOutboxRecord(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, 2, rec.RetryCount)
}

// TestPostgres_SaveCustomerAndCompleteOutbox_Upsert verifies that a second save
// for the same customer_id updates the existing row rather than inserting a new one.
func TestPostgres_SaveCustomerAndCompleteOutbox_Upsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	custID := "shared-cust-" + shortID()

	key1 := "upsert-k1-" + shortID()
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-u1-"+shortID(), key1, `{}`))
	require.NoError(t, testStore.SaveCustomerAndCompleteOutbox(ctx, custID, "Juan", "j@e.com", "EXT-1", key1))

	key2 := "upsert-k2-" + shortID()
	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-u2-"+shortID(), key2, `{}`))
	require.NoError(t, testStore.SaveCustomerAndCompleteOutbox(ctx, custID, "Juan Updated", "j@e.com", "EXT-2", key2))

	c, err := getCustomerFromDB(ctx, custID)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "Juan Updated", c.Name, "second save must update the existing row via ON CONFLICT DO UPDATE")
	assert.Equal(t, "EXT-2", c.ExternalID)

	// Confirm only one row exists for this customer
	conn, connErr := connectDB(t, ctx)
	if connErr == nil {
		defer conn.Close(ctx)
		var count int
		conn.QueryRow(ctx, `SELECT COUNT(*) FROM customers WHERE id=$1`, custID).Scan(&count)
		assert.Equal(t, 1, count, "upsert must not create duplicate customer rows")
	}
}

// TestPostgres_UpdateOutboxStatus_EmptyExtResp_PreservesExisting verifies that
// passing an empty ext_api_response to UpdateOutboxStatus does not overwrite
// the previously stored value (handled by the CASE expression in the SQL).
func TestPostgres_UpdateOutboxStatus_EmptyExtResp_PreservesExisting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	truncateTables(t, ctx)

	key := "preserve-key-" + shortID()
	stored := `{"external_id":"EXT-KEEP"}`

	require.NoError(t, testStore.CreateOutboxRecord(ctx, "id-p-"+shortID(), key, `{}`))
	require.NoError(t, testStore.UpdateOutboxStatus(ctx, key, "EXT_API_DONE", stored))
	require.NoError(t, testStore.UpdateOutboxStatus(ctx, key, "COMPLETED", "")) // empty string

	rec, err := testStore.GetOutboxRecord(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, stored, rec.ExAPIResponse, "empty string must not overwrite the existing ext_api_response")
}

// connectDB opens a raw pgx connection for ad-hoc queries in tests.
func connectDB(t *testing.T, ctx context.Context) (*pgx.Conn, error) {
	t.Helper()
	return pgx.Connect(ctx, testDSN)
}
