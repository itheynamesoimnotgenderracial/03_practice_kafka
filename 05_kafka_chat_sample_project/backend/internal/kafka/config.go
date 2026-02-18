package kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

func NewProducerConfig(brokers, transactionalID string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":   brokers,
		"enable.idempotence":  true,
		"acks":                "all",
		"retries":             10000000,
		"transactional.id":    transactionalID,
		"go.delivery.reports": false,
	}
}

func NewConsumerConfig(brokers, groupID string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	}
}
