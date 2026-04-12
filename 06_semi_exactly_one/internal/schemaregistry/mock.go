package schemaregistry

import "fmt"

// Registry is the interface that AvroCodec depends on.
// Client satisfies it; MockRegistry satisfies it in tests.
type Registry interface {
	EnsureSchema(subject, avscJSON string) (int, error)
	SchemaByID(id int) (string, error)
}

// MockRegistry is an in-memory Registry for unit tests — no HTTP calls needed.
type MockRegistry struct {
	nextID  int
	bySubj  map[string]int
	byID    map[int]string

	ForceEnsureError error
	ForceFetchError  error
}

func NewMockRegistry() *MockRegistry {
	return &MockRegistry{
		nextID: 1,
		bySubj: make(map[string]int),
		byID:   make(map[int]string),
	}
}

func (m *MockRegistry) EnsureSchema(subject, avscJSON string) (int, error) {
	if m.ForceEnsureError != nil {
		return 0, m.ForceEnsureError
	}
	if id, ok := m.bySubj[subject]; ok {
		return id, nil
	}
	id := m.nextID
	m.nextID++
	m.bySubj[subject] = id
	m.byID[id] = avscJSON
	return id, nil
}

func (m *MockRegistry) SchemaByID(id int) (string, error) {
	if m.ForceFetchError != nil {
		return "", m.ForceFetchError
	}
	sc, ok := m.byID[id]
	if !ok {
		return "", fmt.Errorf("mock: schema id %d not found", id)
	}
	return sc, nil
}
