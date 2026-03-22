package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	KafkaBroker       = "localhost:9092"
	SchemaRegistryURL = "http://localhost:8081"
	MongoURI          = "mongodb://localhost:27017"
	MongoDB           = "chat"

	TopicChatRaw       = "chat.raw"
	TopicChatValidated = "chat.validated"
	TopicChatTimeline  = "chat.timeline"
	TopicChatDLT       = "chat.raw.dlt"

	CollectionMessage   = "chat_messages"
	CollectionMetrics   = "room_metrics"
	CollectionSequences = "room_sequences"
)

type ChatRawEvent struct {
	MessageID string `avro:"message_id" json:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id"`
	UserID    string `avro:"user_id" json:"user_id"`
	Content   string `avro:"content" json:"content"`
	Timestamp int64  `avro:"timestamp" json:"timestamp"`
}

type ChatTimelineEvent struct {
	MessageID string `avro:"message_id" json:"message_id"`
	RoomID    string `avro:"room_id" json:"room_id"`
	UserID    string `avro:"user_id" json:"user_id"`
	Content   string `avro:"content" json:"content"`
	Sequence  int64  `avro:"sequence" json:"sequence"`
	Timestamp int64  `avro:"timestamp" json:"timestamp"`
}

type StoreMessage struct {
	MessageID string `bson:"message_id"`
	RoomID    string `bson:"room_id"`
	UserID    string `bson:"user_id"`
	Content   string `bson:"content"`
	Sequence  int64  `bson:"sequence"`
	Timestamp int64  `bson:"timestamp"`
}

type TestInfra struct {
	KafkaProducer *kafka.Producer
	Serializer    *avro.GenericSerializer
	MongoClient   *mongo.Client
	MongoDB       *mongo.Database
	SRClient      schemaregistry.Client
}

func NewTestInfra(t *testing.T) *TestInfra {
	t.Helper()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  KafkaBroker,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		t.Fatalf("failed to create Kafka producer: %v", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(SchemaRegistryURL))
	if err != nil {
		t.Fatalf("failed to create schema registry client: %v", err)
	}

	serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		t.Fatalf("failed to create avro serializer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		t.Fatalf("failed to connect to MongoDB: %v", err)
	}

	if err := mongoClient.Ping(ctx, nil); err != nil {
		t.Fatalf("failed to ping MongoDB: %v", err)
	}

	db := mongoClient.Database(MongoDB)

	return &TestInfra{
		KafkaProducer: producer,
		Serializer:    serializer,
		MongoClient:   mongoClient,
		MongoDB:       db,
		SRClient:      srClient,
	}
}

// Close cleans up all test infrastructure connections.
func (infra *TestInfra) Close() {
	infra.KafkaProducer.Flush(5000)
	infra.KafkaProducer.Close()
	infra.Serializer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	infra.MongoClient.Disconnect(ctx)
}

func (infra *TestInfra) ProduceRawMessage(t *testing.T, roomID, userID, content string) string {
	t.Helper()

	messageID := uuid.New().String()
	event := ChatRawEvent{
		MessageID: messageID,
		RoomID:    roomID,
		UserID:    userID,
		Content:   content,
		Timestamp: time.Now().UnixMilli(),
	}

	serialized, err := infra.Serializer.Serialize("chat.raw-value", &event)
	if err != nil {
		t.Fatalf("failed to serialize message: %v", err)
	}

	topic := TopicChatRaw
	deliveryChan := make(chan kafka.Event)

	err = infra.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(roomID),
		Value: serialized,
	}, deliveryChan)
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		t.Fatalf("message delivery failed: %v", m.TopicPartition.Error)
	}

	t.Logf("Produced message %s to %s[%d]@%d",
		messageID,
		*m.TopicPartition.Topic,
		m.TopicPartition.Partition,
		m.TopicPartition.Offset,
	)

	return messageID
}

// ProduceBatch sends N messages to a room and returns all message IDs.
func (infra *TestInfra) ProduceBatch(t *testing.T, roomID string, count int) []string {
	t.Helper()

	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		content := fmt.Sprintf("test-message-%d-%s", i, uuid.New().String()[:8])
		id := infra.ProduceRawMessage(t, roomID, "test-user", content)
		ids = append(ids, id)
	}
	return ids
}

// ─── MongoDB Queries ───

// GetStoredMessages fetches all messages for a room from chat_messages, sorted by sequence.
func (infra *TestInfra) GetStoredMessages(t *testing.T, roomID string) []StoreMessage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	col := infra.MongoDB.Collection(CollectionMessage)
	opts := options.Find().SetSort(bson.D{{Key: "sequence", Value: 1}})
	cursor, err := col.Find(ctx, bson.M{"room_id": roomID}, opts)
	if err != nil {
		t.Fatalf("failed to query chat_messages: %v", err)
	}
	defer cursor.Close(ctx)

	messages := make([]StoreMessage, 0, len(cursor.Current))
	if err := cursor.All(ctx, &messages); err != nil {
		t.Fatalf("failed to decode messages: %v", err)
	}

	return messages
}

// CountMessagesByID counts how many documents exist with a specific message_id.
// Should always be 0 or 1 with idempotent upserts.
func (infra *TestInfra) CountMessageByID(t *testing.T, messageID string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	col := infra.MongoDB.Collection(CollectionMessage)
	count, err := col.CountDocuments(ctx, bson.M{"message_id": messageID})
	if err != nil {
		t.Fatalf("failed to count messages: %v", err)
	}
	return count
}

// GetSequenceForRoom returns the current sequence counter for a room.
func (infra *TestInfra) GetSequenceForRoom(t *testing.T, roomdID string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	col := infra.MongoDB.Collection(CollectionSequences)
	var result struct {
		Sequence int64 `bson:"sequence"`
	}
	err := col.FindOne(ctx, bson.M{"room_id": roomdID}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0
		}
		t.Fatalf("failed to get sequence: %v", err)
	}
	return result.Sequence
}

// CleanupTestRoom removes all data for a test room from MongoDB.
func (infra *TestInfra) CleanupTestRoom(t *testing.T, roomID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"room_id": roomID}
	infra.MongoDB.Collection(CollectionMessage).DeleteMany(ctx, filter)
	infra.MongoDB.Collection(CollectionMetrics).DeleteMany(ctx, filter)
	infra.MongoDB.Collection(CollectionSequences).DeleteMany(ctx, filter)

	t.Logf("Cleaned up test data for room %s", roomID)
}

// ─── Waiting / Polling ───

// WaitForMessages polls MongoDB until the expected number of messages appear for a room,
// or the timeout is reached.
func (infra *TestInfra) WaitForMessages(t *testing.T, roomID string, expectedCount int, timeout time.Duration) []StoreMessage {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var messages []StoreMessage

	for time.Now().Before(deadline) {
		messages = infra.GetStoredMessages(t, roomID)
		if len(messages) >= expectedCount {
			return messages
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf(
		"timed out waiting for %d messages in room %s (got %d after %v)", expectedCount,
		roomID,
		len(messages),
		timeout,
	)

	return nil
}

// ─── Docker Container Control ───

// DockerExec runs a docker exec command and returns stdout.
func DockerExec(container string, args ...string) (string, error) {
	cmdArgs := append([]string{"exec", container}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// DockerStop stops a container (gracefully with timeout).
func DockerStop(t *testing.T, container string, timeoutSec int) {
	t.Helper()
	cmd := exec.Command("docker", "stop", "-t", fmt.Sprintf("%d", timeoutSec), container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("Warning: docker stop %s failed: %v\n%s", container, err, string(out))
	} else {
		t.Logf("Stopped container %s", container)
	}
}

// DockerStart starts a stopped container.
func DockerStart(t *testing.T, container string) {
	t.Helper()
	cmd := exec.Command("docker", "start", container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker start %s failed: %v\n%s", container, err, string(out))
	}
	t.Logf("Started container %s", container)
}

// DockerRestart restarts a container.
func DockerRestart(t *testing.T, container string) {
	t.Helper()
	cmd := exec.Command("docker", "restart", container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s failed: %v\n%s", container, err, string(out))
	}
	t.Logf("Restarted container %s", container)
}

// DockerNetworkDisconnect disconnects a container from a network.
func DockerNetworkDisconnect(t *testing.T, network, container string) {
	t.Helper()
	cmd := exec.Command("docker", "network", "disconnect", network, container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker network disconnect failed: %v\n%s", err, string(out))
	} else {
		t.Logf("Disconnected %s from %s", container, network)
	}
}

// DockerNetworkConnect reconnects a container to a network.
func DockerNetworkConnect(t *testing.T, network, container string) {
	t.Helper()
	cmd := exec.Command("docker", "network", "connect", network, container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("Warning: network connect failed: %v\n%s", err, string(out))
	} else {
		t.Logf("Reconnected %s to %s", container, network)
	}
}

// IsContainerRunning checks if a Docker container is in running state.
func IsContainerRunning(container string) bool {
	cmd := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}

// WaitForContainer polls until a container is running or timeout.
func WaitForContainer(t *testing.T, container string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if IsContainerRunning(container) {
			t.Logf("Container %s is running", container)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("container %s did not start within %v", container, timeout)
}

// ─── Verification Helpers ───

// VerifyNoDuplicateMessageIDs checks that no message_id appears more than once.
func VerifyNoDuplicateMesageIDs(t *testing.T, messages []StoreMessage) {
	t.Helper()

	seen := make(map[int64]string, len(messages))
	for _, msg := range messages {
		if existing, exists := seen[msg.Sequence]; exists {
			t.Errorf("DUPLICATE SEQUENCE %d: message %s and %s",
				msg.Sequence,
				existing,
				msg.MessageID,
			)
		}
		seen[msg.Sequence] = msg.MessageID
	}
}

// VerifyNoDuplicateSequences checks that no sequence number appears more than once.
func VerifyNoDuplicateSequences(t *testing.T, messages []StoreMessage) {
	t.Helper()

	seen := make(map[int64]string, len(messages))
	for _, msg := range messages {
		if existing, exists := seen[msg.Sequence]; exists {
			t.Errorf("DUPLICATE SEQUENCE %d: message %s and %s",
				msg.Sequence,
				existing,
				msg.MessageID,
			)
		}
		seen[msg.Sequence] = msg.MessageID
	}
}

// VerifySequenceContinuity checks that sequences are contiguous (no gaps)
// starting from startSeq+1 through startSeq+expectedCount.
func VerifySequenceContinuity(t *testing.T, messages []StoreMessage, startSeq int64, expectedCount int) {
	t.Helper()

	if len(messages) != expectedCount {
		t.Errorf("expected %d messages, got %d", expectedCount, len(messages))
	}

	seqSet := make(map[int64]bool, len(messages))
	for _, msg := range messages {
		seqSet[msg.Sequence] = true
	}

	for i := int64(1); i <= int64(expectedCount); i++ {
		expected := startSeq + i
		if !seqSet[expected] {
			t.Errorf("GAP: missing sequence %d", expected)
		}
	}
}

// VerifyAllMessageIDsPresent checks that every produced message_id appears in stored messages.
func VerifyAllMessageIDsPresent(t *testing.T, producedIDs []string, stored []StoreMessage) {
	t.Helper()

	storedSet := make(map[string]bool, len(stored))
	for _, msg := range stored {
		storedSet[msg.MessageID] = true
	}

	for _, id := range producedIDs {
		if !storedSet[id] {
			t.Errorf("MISSING: produced message %s not found in MongoDB:", id)
		}
	}
}

// PrintMessages logs all stored messages for debugging.
func PrintMessages(t *testing.T, messages []StoreMessage) {
	t.Helper()

	data, _ := json.MarshalIndent(messages, "", " ")
	log.Printf("Stored messages:\n%s", string(data))
}
