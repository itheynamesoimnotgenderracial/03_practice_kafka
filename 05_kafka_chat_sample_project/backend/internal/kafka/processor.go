package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Handler func(msg *kafka.Message) ([]ProducerMessage, error)

type ProducerMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

func RunProcessor(
	ctx context.Context,
	consumer *kafka.Consumer,
	producer *TxProducer,
	handler Handler,
) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Closing run processor due to issue:", ctx.Err().Error())
			return nil
		default:
		}

		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Println("read consumer error:", err)
			continue
		}

		log.Printf("received message from %s[%d]@%d\n",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset,
		)

		if err := producer.Begin(); err != nil {
			log.Println("begin tx error:", err)
			continue
		}

		outputs, err := handler(msg)
		if err != nil {
			log.Println("handler msg error:", err)
			producer.Abort(ctx)
			continue
		}

		for _, out := range outputs {
			err := producer.Produce(out.Topic, out.Key, out.Value)
			if err != nil {
				log.Println("output produce error:", err)
				producer.Abort(ctx)
				continue
			}
		}

		consumerMetadata, err := consumer.GetConsumerGroupMetadata()
		if err != nil {
			log.Println("failed to get consumer group metadata:", err)
			producer.Abort(ctx)
			continue
		}

		offsetsToCommit := []kafka.TopicPartition{
			{
				Topic:     msg.TopicPartition.Topic,
				Partition: msg.TopicPartition.Partition,
				Offset:    msg.TopicPartition.Offset + 1,
			},
		}
		err = producer.producer.SendOffsetsToTransaction(ctx, offsetsToCommit, consumerMetadata)
		if err != nil {
			log.Println("error in SendOffsetsToTransaction:", err)
			producer.Abort(ctx)
			continue
		}

		if err := producer.Commit(ctx); err != nil {
			log.Println("commit failed:", err)
			continue
		}

		log.Printf("transaction committed — produced %d message(s)\n", len(outputs))
	}
}
