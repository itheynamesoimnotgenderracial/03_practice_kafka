package db

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockStore is an in-memory Store for unit tests - no Postgres needed.
type MockStore struct {
	mu        sync.RWMutex
	outbox    map[string]*OutboxRecord
	customers map[string]*CustomerRow

	ForceCreateError error
	ForceSaveError   error
}

func NewMockStore() *MockStore {
	return &MockStore{
		outbox:    make(map[string]*OutboxRecord),
		customers: make(map[string]*CustomerRow),
	}
}

func (m *MockStore) Ping(_ context.Context) error { return nil }
func (m *MockStore) Close()                       {}
func (m *MockStore) GetOutboxRecord(_ context.Context, key string) (*OutboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.outbox[key]
	if !ok {
		return nil, nil
	}
	c := *rec
	return &c, nil
}

func (m *MockStore) CreateOutboxRecord(_ context.Context, id, key, payload string) error {
	if m.ForceCreateError != nil {
		return m.ForceCreateError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.outbox[key]; exists {
		return fmt.Errorf("outbox record already exists: %s", key)
	}
	m.outbox[key] = &OutboxRecord{
		ID:             id,
		IdempotencyKey: key,
		Status:         "PENDING",
		Payload:        payload,
	}
	return nil
}

func (m *MockStore) UpdateOutboxStatus(_ context.Context, key, status, extResp string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[key]
	if !ok {
		return fmt.Errorf("outbox not found: %s", key)
	}
	rec.Status = status
	if extResp != "" {
		rec.ExAPIResponse = extResp
	}
	return nil
}

func (m *MockStore) MarkOutboxFailed(_ context.Context, key, step, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[key]
	if !ok {
		return nil
	}
	rec.Status = "FAILED"
	rec.FailedAtStep = step
	rec.FailureReason = reason
	rec.RetryCount++
	return nil
}

func (m *MockStore) IncrementRetryCount(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[key]
	if !ok {
		return fmt.Errorf("outbox not found: %s", key)
	}
	rec.RetryCount++
	return nil
}

func (m *MockStore) SaveCustomerAndCompleteOutbox(_ context.Context, customerID, name, email, externalID, key string) error {
	if m.ForceSaveError != nil {
		return m.ForceSaveError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[key]
	if !ok {
		return fmt.Errorf("outbox missing: %s", key)
	}
	m.customers[customerID] = &CustomerRow{
		ID:         customerID,
		Name:       name,
		Email:      email,
		ExternalID: externalID,
		CreatedAt:  time.Now(),
	}
	rec.Status = "COMPLETED"
	return nil
}

func (m *MockStore) GetCustomer(id string) *CustomerRow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.customers[id]
	if !ok {
		return nil
	}
	cp := *c
	return &cp
}

func (m *MockStore) AllCustomers() []*CustomerRow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*CustomerRow, 0, len(m.customers))
	for _, v := range m.customers {
		cp := *v
		out = append(out, &cp)
	}
	return out
}

func (m *MockStore) Seed(rec OutboxRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outbox[rec.IdempotencyKey] = &rec
}
