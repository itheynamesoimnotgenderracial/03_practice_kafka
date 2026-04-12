package db_test

import (
	"context"
	"simple-semo-eos/internal/db"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// All DB unit tests run againts MockStore - no Postgres required.
// Integration tests (integration/) test the real PostgresStore via Docker.

var ctx = context.Background()

// ── GetOutboxRecord ───────────────────────────────────────────────────────────

func TestGetOutboxRecord_NotFound_ReturnsNil(t *testing.T) {
	s := db.NewMockStore()
	rec, err := s.GetOutboxRecord(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestGetOutboxRecord_found_ReturnsRecord(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{"foo": "bar"}`))

	rec, err := s.GetOutboxRecord(ctx, "key-1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "PENDING", rec.Status)
	assert.Equal(t, "key-1", rec.IdempotencyKey)
}

func TestGetOutboxRecord_ReturnsCopy_MutationDoesNotAffectStore(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-key", "key-copy", `{}`))

	rec, _ := s.GetOutboxRecord(ctx, "key-copy")
	rec.Status = "MUTATED"

	rec2, _ := s.GetOutboxRecord(ctx, "key-copy")
	assert.Equal(t, "PENDING", rec2.Status, "store must not be affected by mutating the returned copy")
}

// ── CreateOutboxRecord ────────────────────────────────────────────────────────

func TestCreateOutboxRecord_Success(t *testing.T) {
	s := db.NewMockStore()
	assert.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
}

func TestCreateOutboxRecord_DefaultStatus_IsPending(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, "PENDING", rec.Status)
}

func TestCreateOutboxRecord_DuplicateKey_ReturnsError(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "dup", `{}`))
	err := s.CreateOutboxRecord(ctx, "id-2", "dup", `{}`)
	assert.Error(t, err, "second insert with same key must fail")
}

// ── UpdateOutboxStatus ────────────────────────────────────────────────────────

func TestUpdateOutboxStatus_PendingToExtAPIDone(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))

	extJSON := `{"external_id":"EXT-1","valid":true}`
	require.NoError(t, s.UpdateOutboxStatus(ctx, "key-1", "EXT_API_DONE", extJSON))

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, "EXT_API_DONE", rec.Status)
	assert.Equal(t, extJSON, rec.ExAPIResponse)
}

func TestUpdateOutboxStatus_ExtAPIDoneToCompleted(t *testing.T) {
	s := db.NewMockStore()
	assert.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	assert.NoError(t, s.UpdateOutboxStatus(ctx, "key-1", "EXT_API_DONE", `{}`))
	require.NoError(t, s.UpdateOutboxStatus(ctx, "key-1", "COMPLETED", ""))

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, "COMPLETED", rec.Status)
}

func TestUpdateOutboxStatus_NonExistentKey_ReturnsError(t *testing.T) {
	s := db.NewMockStore()
	assert.Error(t, s.UpdateOutboxStatus(ctx, "ghost", "COMPLETED", ""))
}

func TestUpdateOutboxStatus_EmptyExtResponse_PreservesExisting(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	stored := `{"external_id":"EXT-KEEP"}`
	require.NoError(t, s.UpdateOutboxStatus(ctx, "key-1", "EXT_API_DONE", stored))
	require.NoError(t, s.UpdateOutboxStatus(ctx, "key-1", "COMPLETED", ""))

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, stored, rec.ExAPIResponse, "empty string must not overwrite existing response")
}

// ── MarkOutboxFailed ──────────────────────────────────────────────────────────

func TestMarkOutboxFailed_SetsAllFields(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	s.MarkOutboxFailed(ctx, "key-1", "EXTERNAL_API", "timeout")

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, "FAILED", rec.Status)
	assert.Equal(t, "EXTERNAL_API", rec.FailedAtStep)
	assert.Equal(t, "timeout", rec.FailureReason)
	assert.Equal(t, 1, rec.RetryCount)
}

func TestMarkOutboxFailed_IncrementsRetryCount_OnEachCall(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	s.MarkOutboxFailed(ctx, "key-1", "EXTERNAL_API", "err")
	s.UpdateOutboxStatus(ctx, "key-1", "PENDING", "")
	s.MarkOutboxFailed(ctx, "key-1", "EXTERNAL_API", "err")

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, 2, rec.RetryCount)
}

func TestMarkOutboxFailed_NonExistentKey_IsNoOp(t *testing.T) {
	s := db.NewMockStore()
	assert.NotPanics(t, func() {
		s.MarkOutboxFailed(ctx, "ghost", "DB_SAVE", "err")
	})
}

// ── SaveCustomerAndCompleteOutbox ─────────────────────────────────────────────

func TestSaveCustomerAndCompleteOutbox_SavesCustomerAndCompleteOutbox(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))

	err := s.SaveCustomerAndCompleteOutbox(ctx, "cust-1", "Juan", "juan@example.com", "EXT-001", "key-1")
	require.NoError(t, err)

	rec, _ := s.GetOutboxRecord(ctx, "key-1")
	assert.Equal(t, "COMPLETED", rec.Status)

	c := s.GetCustomer("cust-1")
	require.NotNil(t, c)
	assert.Equal(t, "Juan", c.Name)
	assert.Equal(t, "EXT-001", c.ExternalID)
}

func TestSaveCustomerAndCompleteOutbox_MissingOutboxKey_ReturnsError(t *testing.T) {
	s := db.NewMockStore()
	err := s.SaveCustomerAndCompleteOutbox(ctx, "cust-99", "Ghost", "g@test.com", "EXT-999", "missing-key")
	assert.Error(t, err)
}

func TestSaveCustomerAndCompleteOutbox_ForceSaveError_Propagates(t *testing.T) {
	s := db.NewMockStore()
	s.ForceSaveError = assert.AnError
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	assert.Error(t, s.SaveCustomerAndCompleteOutbox(ctx, "c1", "J", "j@e.com", "E1", "key-1"))
}

func TestSaveCustomerAndCompleteOutbox_SameCustomerID_Upserts(t *testing.T) {
	s := db.NewMockStore()
	require.NoError(t, s.CreateOutboxRecord(ctx, "id-1", "key-1", `{}`))
	require.NoError(t, s.SaveCustomerAndCompleteOutbox(ctx, "cust-1", "Juan", "j@e.com", "EXT-1", "key-1"))

	s.Seed(db.OutboxRecord{ID: "id-2", IdempotencyKey: "key-2", Status: "EXT_API_DONE"})
	require.NoError(t, s.SaveCustomerAndCompleteOutbox(ctx, "cust-1", "Juan Updated", "j@e.com", "EXT-2", "key-2"))

	assert.Len(t, s.AllCustomers(), 1, "upsert must not create a second row for the same customer")
}

// ── Seed ──────────────────────────────────────────────────────────────────────

func TestSeed_SetsStatusDirectly(t *testing.T) {
	s := db.NewMockStore()
	s.Seed(db.OutboxRecord{
		ID:             "seeded",
		IdempotencyKey: "seeded-key",
		Status:         "EXT_API_DONE",
		ExAPIResponse:  `{"external_id":"EXT-SEEDED"}`,
	})

	rec, err := s.GetOutboxRecord(ctx, "seeded-key")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "EXT_API_DONE", rec.Status)
	assert.Contains(t, rec.ExAPIResponse, "EXT-SEEDED")
}
