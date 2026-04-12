package integration_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
	"simple-semo-eos/internal/db"
	ikafka "simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/schemaregistry"
	"simple-semo-eos/internal/serde"
)

// Package-level state set up once by TestMain and shared across all tests.
var (
	testBrokers         string
	testDSN             string
	testSRURL           string
	testValidationTopic string
	testConsumerTopic   string
	testDLQTopic        string
	testStore           *db.PostgresStore
	testCodec           *serde.AvroCodec
)

// TestMain boots shared infrastructure once for all integration tests.
// Run with:
//
//	INTEGRATION=1 go test ./tests/integration/ -v -timeout 5m
func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		fmt.Println("skipping integration tests: set INTEGRATION=1 to enable")
		os.Exit(0)
	}

	testBrokers = envOrDefault("KAFKA_BROKERS", "localhost:9092")
	testDSN = envOrDefault("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/customerdb?sslmode=disable")
	testSRURL = envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	// Unique suffix so topics from parallel runs never collide.
	runID := shortID()
	testValidationTopic = "test.validator." + runID
	testConsumerTopic = "test.consumer." + runID
	testDLQTopic = "test.dlq." + runID

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var err error
	testStore, err = db.NewPostgresStore(ctx, testDSN)
	if err != nil {
		log.Fatalf("integration: postgres: %v", err)
	}

	if err := createTopics(testBrokers, testValidationTopic, testConsumerTopic, testDLQTopic); err != nil {
		log.Fatalf("integration: create topics: %v", err)
	}

	srClient := schemaregistry.New(testSRURL)
	testCodec = serde.NewAvroCodec(srClient)
	for _, s := range []struct{ subject, avsc string }{
		{testValidationTopic + "-value", serde.CustomerMessageAvsc},
		{testConsumerTopic + "-value", serde.CustomerMessageAvsc},
		{testDLQTopic + "-value", serde.DLQMessageAvsc},
	} {
		if err := testCodec.Register(s.subject, s.avsc); err != nil {
			log.Fatalf("integration: register schema %s: %v", s.subject, err)
		}
	}

	code := m.Run()
	testStore.Close()
	os.Exit(code)
}

// ── Infrastructure helpers ────────────────────────────────────────────────────

func createTopics(brokers string, topics ...string) error {
	a, err := ck.NewAdminClient(&ck.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer a.Close()

	specs := make([]ck.TopicSpecification, len(topics))
	for i, topic := range topics {
		specs[i] = ck.TopicSpecification{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 3,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := a.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("CreateTopics: %w", err)
	}
	for _, r := range results {
		if r.Error.Code() != ck.ErrNoError && r.Error.Code() != ck.ErrTopicAlreadyExists {
			return fmt.Errorf("create topic %s: %v", r.Topic, r.Error)
		}
	}
	return nil
}

// ── Per-test helpers ──────────────────────────────────────────────────────────

func mustProducer(t *testing.T) *ikafka.ConfluentProducer {
	t.Helper()
	p, err := ikafka.NewConfluentProducer(testBrokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func mustConsumer(t *testing.T, groupID string) *ikafka.ConfluentConsumer {
	t.Helper()
	c, err := ikafka.NewConfluentConsumer(testBrokers, groupID)
	if err != nil {
		t.Fatalf("consumer %s: %v", groupID, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// truncateTables wipes outbox and customers so each test starts clean.
func truncateTables(t *testing.T, ctx context.Context) {
	t.Helper()
	conn, err := pgx.Connect(ctx, testDSN)
	if err != nil {
		t.Fatalf("truncate connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "TRUNCATE TABLE outbox, customers"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// getCustomerFromDB fetches a customer row directly via pgx (Store interface
// does not expose a read-customer method).
func getCustomerFromDB(ctx context.Context, customerID string) (*db.CustomerRow, error) {
	conn, err := pgx.Connect(ctx, testDSN)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	var c db.CustomerRow
	err = conn.QueryRow(ctx,
		`SELECT id, name, email, COALESCE(external_id,'') FROM customers WHERE id=$1`,
		customerID,
	).Scan(&c.ID, &c.Name, &c.Email, &c.ExternalID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &c, err
}

// pollUntil checks cond every 250 ms until it returns true or ctx expires.
func pollUntil(t *testing.T, ctx context.Context, cond func() bool) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for condition")
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func shortID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
