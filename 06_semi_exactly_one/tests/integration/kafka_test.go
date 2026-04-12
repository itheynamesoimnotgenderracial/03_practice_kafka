package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ikafka "simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
)

// TestKafka_ProduceAndConsume_RoundTrip verifies that a message published with
// the real ConfluentProducer can be polled back by a ConfluentConsumer.
func TestKafka_ProduceAndConsume_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	producer := mustProducer(t)
	consumer := mustConsumer(t, "test-roundtrip-"+shortID())

	msg := models.CustomerMessage{
		MessageID:  shortID(),
		CustomerID: "rt-cust-" + shortID(),
		Name:       "Round Trip",
		Email:      "rt@test.com",
		Action:     "CREATE",
	}
	payload, err := json.Marshal(msg)
	require.NoError(t, err)

	require.NoError(t, consumer.Subscribe([]string{testValidationTopic}))
	require.NoError(t, producer.Publish(testValidationTopic, msg.CustomerID, payload))

	received, err := ikafka.PollWithTimeout(consumer, 15*time.Second)
	require.NoError(t, err)
	require.NotNil(t, received, "consumer must receive the published message within the timeout")
	assert.Equal(t, testValidationTopic, received.Topic)

	var got models.CustomerMessage
	require.NoError(t, json.Unmarshal(received.Value, &got))
	assert.Equal(t, msg.CustomerID, got.CustomerID)
	assert.Equal(t, msg.Name, got.Name)
	assert.Equal(t, msg.Action, got.Action)

	_ = ctx
}

// TestKafka_AvroCodec_SerializeAndDeserialize verifies that the real AvroCodec
// (backed by the live Schema Registry) produces a valid Confluent wire format
// payload and that the same codec can decode it back to the original struct.
func TestKafka_AvroCodec_SerializeAndDeserialize(t *testing.T) {
	msg := models.CustomerMessage{
		MessageID:  shortID(),
		CustomerID: "avro-cust-" + shortID(),
		Name:       "Avro Test",
		Email:      "avro@test.com",
		Action:     "UPDATE",
	}

	subject := testValidationTopic + "-value"
	payload, err := testCodec.Serialize(subject, msg)
	require.NoError(t, err)

	// Confluent wire format: [0x00][4-byte schema ID][avro bytes]
	require.Greater(t, len(payload), 5, "serialized payload must be longer than the 5-byte wire format header")
	assert.Equal(t, byte(0x00), payload[0], "first byte must be the Confluent magic byte 0x00")

	var got models.CustomerMessage
	require.NoError(t, testCodec.Deserialize(payload, &got))
	assert.Equal(t, msg.CustomerID, got.CustomerID)
	assert.Equal(t, msg.Name, got.Name)
	assert.Equal(t, msg.Action, got.Action)
}

// TestKafka_CommitMessage_DoesNotError verifies that CommitMessage succeeds on
// a message returned from Poll (i.e., the raw confluent-kafka-go handle is
// wired correctly through our Message wrapper).
func TestKafka_CommitMessage_DoesNotError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	producer := mustProducer(t)
	consumer := mustConsumer(t, "test-commit-"+shortID())

	payload, _ := json.Marshal(models.CustomerMessage{
		MessageID:  shortID(),
		CustomerID: "commit-cust-" + shortID(),
		Name:       "Commit Test",
		Email:      "commit@test.com",
		Action:     "CREATE",
	})

	require.NoError(t, consumer.Subscribe([]string{testValidationTopic}))
	require.NoError(t, producer.Publish(testValidationTopic, "commit-cust", payload))

	msg, err := ikafka.PollWithTimeout(consumer, 15*time.Second)
	require.NoError(t, err)
	require.NotNil(t, msg)

	assert.NoError(t, consumer.CommitMessage(msg))

	_ = ctx
}
