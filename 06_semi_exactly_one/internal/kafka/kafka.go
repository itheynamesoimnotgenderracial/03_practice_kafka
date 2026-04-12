package kafka

import (
	"fmt"
	"log"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer interface {
	Publish(topic, key string, value []byte) error
	Close() error
}

type Consumer interface {
	Subscribe(topics []string) error
	Poll(timeoutMs int) (*Message, error)
	CommitMessage(msg *Message) error
	Close() error
}

// Message is our broker-agnostic message type
type Message struct {
	Topic string
	Key   []byte
	Value []byte
	raw   *ck.Message
}

type ConfluentProducer struct {
	p      *ck.Producer
	broker string
}

func NewConfluentProducer(brokers string) (*ConfluentProducer, error) {
	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*ck.Message); ok && msg.TopicPartition.Error != nil {
				log.Printf("[Kafka] delivery error: %v", msg.TopicPartition.Error)
			}
		}
	}()
	return &ConfluentProducer{p: p, broker: brokers}, nil
}

func (cp *ConfluentProducer) Brokers() string { return cp.broker }
func (cp *ConfluentProducer) Publish(topic, key string, value []byte) error {
	var kb []byte
	if key != "" {
		kb = []byte(key)
	}
	err := cp.p.Produce(&ck.Message{
		TopicPartition: ck.TopicPartition{
			Topic:     &topic,
			Partition: ck.PartitionAny,
		},
		Key:   kb,
		Value: value,
	}, nil)
	if err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}

	if rem := cp.p.Flush(5000); rem > 0 {
		return fmt.Errorf("kafka flush timeout: %d message remaining", rem)
	}
	return nil
}

type ConfluentConsumer struct {
	c *ck.Consumer
}

func NewConfluentConsumer(brokers, groupID string) (*ConfluentConsumer, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &ConfluentConsumer{c: c}, nil
}

func (cc *ConfluentConsumer) Subscribe(topics []string) error {
	return cc.c.SubscribeTopics(topics, nil)
}

func (cc *ConfluentConsumer) Poll(timeoutMs int) (*Message, error) {
	ev := cc.c.Poll(timeoutMs)
	if ev == nil {
		return nil, nil
	}

	switch e := ev.(type) {
	case *ck.Message:
		topic := ""
		if e.TopicPartition.Topic != nil {
			topic = *e.TopicPartition.Topic
		}
		return &Message{Topic: topic, Key: e.Key, Value: e.Value, raw: e}, nil
	case ck.Error:
		if e.Code() == ck.ErrAllBrokersDown {
			return nil, fmt.Errorf("all brokers down: %w", e)
		}
		log.Printf("[Kafka] warning: %v", e)
		return nil, nil
	}
	return nil, nil
}

func (cc *ConfluentConsumer) CommitMessage(msg *Message) error {
	if msg == nil || msg.raw == nil {
		return nil
	}
	_, err := cc.c.CommitMessage(msg.raw)
	return err
}

func (cp *ConfluentProducer) Close() error { cp.p.Close(); return nil }
func (cc *ConfluentConsumer) Close() error { return cc.c.Close() }

// PollWithTimeout polls until a message arrives or timeout expires.
func PollWithTimeout(c Consumer, timeout time.Duration) (*Message, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, err := c.Poll(100)
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
	}
	return nil, fmt.Errorf("no message within %s", timeout)
}
