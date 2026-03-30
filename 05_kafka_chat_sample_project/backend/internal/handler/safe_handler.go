package handler

import (
	"fmt"
	"log"
	"runtime/debug"
	"sample-chat/internal/dlt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type HandlerOutput struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []kafka.Header
}

// SafeHandler wraps a handler function with panic recovery.
// If the handler panics, the message is routed to the DLT topic
// instead of crashing the entire consumer process.
//
// Usage:
//
//	safeHandler := handler.SafeHandler(myHandler)
//	outputs, err := safeHandler(msg)
func SafeHandler(h func(*kafka.Message) ([]HandlerOutput, error)) func(*kafka.Message) ([]HandlerOutput, error) {
	return func(msg *kafka.Message) (outputs []HandlerOutput, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				reason := fmt.Sprintf("panic: %v", r)

				log.Printf("🚨 PANIC RECOVERED in handler: %s\nStack trace:\n%s", reason, stack)

				// Route the poison pill to DLT with panic metadata
				dltTopic := dlt.TopicChatRawDLT
				failMsg := &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &dltTopic,
						Partition: kafka.PartitionAny,
					},
					Key:   msg.Key,
					Value: msg.Value,
				}

				dlt.SetRetryHeaders(failMsg, 0, topicName(msg), reason, dlt.ErrorTypePanic)

				outputs = []HandlerOutput{
					{
						Topic:   dltTopic,
						Key:     msg.Key,
						Value:   failMsg.Value,
						Headers: failMsg.Headers,
					},
				}
				err = nil
			}
		}()

		return h(msg)
	}
}

// SafeHandlerWithRetry wraps a handler with both panic recovery AND retry routing.
// Deserialization errors → DLT (permanent, no retry).
// Panics → DLT (permanent, no retry).
// Transient errors → retry topic (if under max retries) or DLT.
func SafeHandlerWithRetry(h func(*kafka.Message) ([]HandlerOutput, error)) func(*kafka.Message) ([]HandlerOutput, error) {
	return func(msg *kafka.Message) (outputs []HandlerOutput, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				reason := fmt.Sprintf("panic: %v", r)
				messageID := dlt.GetHeader(msg, dlt.HeaderMessageID)
				log.Printf("🚨 PANIC RECOVERED: %s\nStack:\n%s", reason, stack)

				destTopic, retryCount := dlt.RouteToRetryOrDLT(msg, dlt.ErrorTypePanic)
				failMsg := dlt.BuildFailureMessage(msg, destTopic, retryCount, reason, dlt.ErrorTypePanic, messageID)

				outputs = []HandlerOutput{
					{
						Topic:   destTopic,
						Key:     failMsg.Key,
						Value:   failMsg.Value,
						Headers: failMsg.Headers,
					},
				}
				err = nil
			}
		}()

		return h(msg)
	}
}

func topicName(msg *kafka.Message) string {
	if msg.TopicPartition.Topic != nil {
		return *msg.TopicPartition.Topic
	}

	return "unknown"
}
