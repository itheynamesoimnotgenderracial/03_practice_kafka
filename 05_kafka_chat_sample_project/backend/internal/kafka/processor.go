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

		err = producer.producer.SendOffsetsToTransaction(ctx, []kafka.TopicPartition{msg.TopicPartition}, &kafka.ConsumerGroupMetadata{})
		if err != nil {
			log.Println("error in SendOffsetsToTransaction:", err)
			producer.Abort(ctx)
			continue
		}

		if err := producer.Commit(ctx); err != nil {
			log.Println("commit failed:", err)
			continue
		}
	}
}
