package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TxProducerStore interface {
	Begin() error
	Produce(topic string, key, value []byte) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

type TxProducer struct {
	producer *kafka.Producer
	inTx     bool
}

func NewTxProducer(cfg *kafka.ConfigMap) (TxProducerStore, error) {
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	err = p.InitTransactions(ctx)
	if err != nil {
		return nil, err
	}

	return &TxProducer{producer: p}, nil
}

func (p *TxProducer) Begin() error {
	return p.producer.BeginTransaction()
}

func (p *TxProducer) Produce(topic string, key, value []byte) error {
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: value,
	}, nil)
}

func (p *TxProducer) Commit(ctx context.Context) error {
	return p.producer.CommitTransaction(ctx)
}

func (p *TxProducer) Abort(ctx context.Context) error {
	return p.producer.AbortTransaction(ctx)
}
