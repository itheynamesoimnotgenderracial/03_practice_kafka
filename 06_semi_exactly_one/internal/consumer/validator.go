package consumer

import (
	"context"
	"errors"
	"log"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
	"simple-semo-eos/internal/serde"
)

type ValidatorConfig struct {
	Consumer        kafka.Consumer
	Producer        kafka.Producer
	Serde           serde.Codec
	CustomerTopic   string
	DLQTopic        string
	ValidationTopic string
}

// RunValidatorConsumer subscribes to ValidationTopic, validates each message,
// then forwards to CustomerTopic or DLQTopic.
func RunValidatorConsumer(ctx context.Context, cfg ValidatorConfig) error {
	if err := cfg.Consumer.Subscribe([]string{cfg.ValidationTopic}); err != nil {
		return err
	}
	log.Printf("[Validator] subscribed to %s", cfg.ValidationTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Validator] shutting down")
			return cfg.Consumer.Close()
		default:
		}

		kmsg, err := cfg.Consumer.Poll(200)
		if err != nil {
			log.Printf("[Validator] poll error: %v", err)
			continue
		}
		if kmsg == nil {
			continue
		}

		// Deserialize Confluent Avro wire format → CustomerMessage
		var msg models.CustomerMessage
		if err := cfg.Serde.Deserialize(kmsg.Value, &msg); err != nil {
			sendToDLQ(ctx, cfg.Producer, cfg.Serde, cfg.DLQTopic, models.DLQMessage{
				FailedAtStep: "VALIDATION_DESERIALIZATION",
				Reason:       err.Error(),
			})
			if commitErr := cfg.Consumer.CommitMessage(kmsg); commitErr != nil {
				log.Printf("[Validator] x commit after DLQ error: %v", commitErr)
			}
			continue
		}

		// Re-serialize for the consumer topic (same schema, different subject)
		forwardPayload, err := cfg.Serde.Serialize(cfg.CustomerTopic+"-value", msg)
		if err != nil {
			log.Printf("[Validator] x serialize for forward: %v", err)
			continue
		}

		if err := cfg.Producer.Publish(cfg.CustomerTopic, msg.CustomerID, forwardPayload); err != nil {
			log.Printf("[Validator] x forward failed: %v", err)
			continue
		}

		if err := cfg.Consumer.CommitMessage(kmsg); err != nil {
			log.Printf("[Validator] x error when committing message: %v", err)
		}
		log.Printf("[Validator] ✓ forwarded | message_id=%s", msg.MessageID)
	}
}

// ValidateMessage checks required fields and allowed actions. Exported so
// export_test.go can expose it to the test package.
func ValidateMessage(msg models.CustomerMessage) error {
	if msg.CustomerID == "" {
		return errors.New("customer_id is required")
	}
	if msg.Name == "" {
		return errors.New("name is required")
	}
	if msg.Email == "" {
		return errors.New("email is required")
	}
	if msg.Action != "CREATE" && msg.Action != "UPDATE" {
		return errors.New("action must be CREATE or UPDATE, got: " + msg.Action)
	}
	return nil
}
