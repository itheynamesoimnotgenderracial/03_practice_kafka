package serde

import "fmt"

// MockCodec implements Codec with controllable failure injection for tests.
// Use ForceSerializeError / ForceDeserializeError to simulate codec failures.
// For simple tests that just need JSON round-tripping, prefer JSONCodec instead.
type MockCodec struct {
	inner                Codec
	ForceSerializeError   error
	ForceDeserializeError error
}

// NewMockCodec returns a MockCodec that delegates to JSONCodec by default.
func NewMockCodec() *MockCodec {
	return &MockCodec{inner: JSONCodec{}}
}

func (m *MockCodec) Serialize(subject string, v interface{}) ([]byte, error) {
	if m.ForceSerializeError != nil {
		return nil, fmt.Errorf("mock codec: serialize: %w", m.ForceSerializeError)
	}
	return m.inner.Serialize(subject, v)
}

func (m *MockCodec) Deserialize(data []byte, v interface{}) error {
	if m.ForceDeserializeError != nil {
		return fmt.Errorf("mock codec: deserialize: %w", m.ForceDeserializeError)
	}
	return m.inner.Deserialize(data, v)
}
