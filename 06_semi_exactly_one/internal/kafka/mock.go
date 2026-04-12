package kafka

import (
	"fmt"
	"sync"
)

type MockProducer struct {
	mu          sync.Mutex
	messages    []PublishMessage
	FailOnTopic string
}

type PublishMessage struct {
	Topic string
	Key   string
	Value []byte
}

func NewMockProducer() *MockProducer { return &MockProducer{} }

func (m *MockProducer) Publish(topic, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailOnTopic == topic {
		return fmt.Errorf("mock: forced failure on topic: %s", topic)
	}
	m.messages = append(m.messages, PublishMessage{Topic: topic, Key: key, Value: value})
	return nil
}

func (m *MockProducer) Close() error { return nil }

func (m *MockProducer) Published() []PublishMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PublishMessage, len(m.messages))
	copy(out, m.messages)
	return out
}

func (m *MockProducer) PublishedTo(topic string) []PublishMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []PublishMessage
	for _, msg := range m.messages {
		if msg.Topic == topic {
			out = append(out, msg)
		}
	}
	return out
}

func (m *MockProducer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = nil
}

// ── MockConsumer ─────────────────────────────────────────────────────────────

type MockConsumer struct {
	mu        sync.Mutex
	queue     []*Message
	committed []*Message
}

func NewMockConsumer() *MockConsumer { return &MockConsumer{} }

func (m *MockConsumer) Push(topic string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, &Message{Topic: topic, Value: value})
}

func (m *MockConsumer) Subscribe(_ []string) error { return nil }
func (m *MockConsumer) Poll(_ int) (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queue) == 0 {
		return nil, nil
	}
	msg := m.queue[0]
	m.queue = m.queue[1:]
	return msg, nil
}

func (m *MockConsumer) CommitMessage(msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed = append(m.committed, msg)
	return nil
}

func (m *MockConsumer) Close() error { return nil }
func (m *MockConsumer) Committed() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Message, len(m.committed))
	copy(out, m.committed)
	return nil
}
