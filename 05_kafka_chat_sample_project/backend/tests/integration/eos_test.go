package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Broker Crash Mid-Transaction
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   1. Send a batch of messages to chat.raw
//   2. While the pipeline is processing, kill kafka2 (a follower broker)
//   3. Wait for the pipeline to recover (Kafka rebalances to remaining brokers)
//   4. Send another batch after recovery
//   5. Verify: no duplicates, no gaps, all messages persisted exactly once
//
// Why this works:
//   Your cluster has 3 brokers with RF=3 and min.insync.replicas=2.
//   Killing one broker still leaves 2 in-sync replicas, which satisfies ISR.
//   The transactional producers in chat-processor and timeline-processor
//   should handle the broker failure transparently.
//
// Container names: kafka1, kafka2, kafka3

func TestBrokerCrashMidTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	infra := NewTestInfra(t)
	defer infra.Close()

	roomID := fmt.Sprintf("eos-broker-crash-%s", uuid.New().String()[:8])
	defer infra.CleanupTestRoom(t, roomID)

	// Get the starting sequence for this room (should be 0 for a new room)
	startSeq := infra.GetSequenceForRoom(t, roomID)
	t.Logf("Starting sequence for room %s: %d", roomID, startSeq)

	// Phase 1: Send first batch before the crash
	batchSize1 := 5
	t.Logf("Phase 1: Sending %d messages before broker crash", batchSize1)
	ids1 := infra.ProduceBatch(t, roomID, batchSize1)

	// Wait for first batch to be fully processed
	infra.WaitForMessages(t, roomID, batchSize1, 30*time.Second)
	t.Log("Phase 1 complete: first batch processed")

	// Phase 2: Kill kafka2 (follower broker)
	t.Log("Phase 2: Killing kafka2...")
	DockerStop(t, "kafka2", 1)

	// Give the cluster a moment to detect the failure
	time.Sleep(3 * time.Second)

	// Phase 3: Send second batch to detect while broker is down
	batchSize2 := 5
	t.Logf("Phase 3: Sending %d messages with kafka2 down", batchSize2)
	ids2 := infra.ProduceBatch(t, roomID, batchSize2)

	// Phase 4: Restart kafka2
	t.Log("Phase 4: Restarting kafka2...")
	DockerStart(t, "kafka2")
	WaitForContainer(t, "kafka2", 30*time.Second)

	// Give broker time to rejoin ISR
	time.Sleep(10 * time.Second)

	// Phase 5: Wait for all messages and verify
	totalMessages := batchSize1 + batchSize2
	t.Logf("Phase 5: Waiting for all %d messages...", totalMessages)
	messages := infra.WaitForMessages(t, roomID, totalMessages, 60*time.Second)

	// ─── Exactly-Once Verification ───
	t.Log("Verifying exactly-once guarantees...")

	allIDs := append(ids1, ids2...)
	VerifyAllMessageIDsPresent(t, allIDs, messages)
	VerifyNoDuplicateMesageIDs(t, messages)
	VerifyNoDuplicateSequences(t, messages)
	VerifySequenceContinuity(t, messages, startSeq, totalMessages)

	t.Logf("✅ Broker crash test passed: %d messages, no duplicates, no gaps", len(messages))
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Consumer Crash and Rebalance
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   1. Send a batch of messages
//   2. Wait for them to be processed
//   3. Restart the timeline-processor container (simulates consumer crash)
//   4. Send another batch after the processor comes back
//   5. Verify: the processor picks up exactly where it left off, no duplicates
//
// This tests Kafka's consumer group rebalance + exactly-once offset tracking.
// The timeline-processor uses sendOffsetsToTransaction to atomically commit
// offsets within the transaction. On restart, it should resume from the last
// committed offset.

func TestConsumerCrashAndRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	infra := NewTestInfra(t)
	defer infra.Close()

	roomID := fmt.Sprintf("eos-consumer-crash-%s", uuid.New().String()[:8])
	defer infra.CleanupTestRoom(t, roomID)

	startSeq := infra.GetSequenceForRoom(t, roomID)

	// Phase 1: Send and Process first batch
	batchSize1 := 5
	t.Logf("Phase 1: Sending %d messages", batchSize1)
	ids1 := infra.ProduceBatch(t, roomID, batchSize1)
	infra.WaitForMessages(t, roomID, batchSize1, 30*time.Second)

	// Phase 2: Restart the timeline-processor (simulates crash)
	t.Log("Phase 2: Restarting timeline-processor...")
	DockerRestart(t, "timeline-processor")

	// Wait for the processor to fully restart and rejoin consumer group
	WaitForContainer(t, "timeline-processor", 30*time.Second)
	time.Sleep(15 * time.Second)

	// Phase 3: Send second batch
	batchSize2 := 5
	t.Logf("Phase 3: Sending %d messages after processor restart", batchSize2)
	ids2 := infra.ProduceBatch(t, roomID, batchSize2)

	// Phase 4: Wait for all messages
	totalMessages := batchSize1 + batchSize2
	messages := infra.WaitForMessages(t, roomID, totalMessages, 60*time.Second)

	// ─── Exactly-Once Verification ───
	t.Log("Verifying exactly-once guarantees...")

	allIDs := append(ids1, ids2...)
	VerifyAllMessageIDsPresent(t, allIDs, messages)
	VerifyNoDuplicateMesageIDs(t, messages)
	VerifyNoDuplicateSequences(t, messages)
	VerifySequenceContinuity(t, messages, startSeq, totalMessages)

	finalSeq := infra.GetSequenceForRoom(t, roomID)
	expectedSeq := startSeq + int64(totalMessages)
	if finalSeq != expectedSeq {
		t.Errorf("expected final sequence %d, got %d", expectedSeq, finalSeq)
	}

	t.Logf("✅ Consumer crash test passed: %d messages, sequence integrity verified", len(messages))
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Network Partition
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   1. Send a batch of messages and verify processing
//   2. Disconnect kafka3 from the Docker network (simulates network partition)
//   3. Send another batch — should still work (2/3 brokers available, ISR=2)
//   4. Reconnect kafka3
//   5. Send a final batch after healing
//   6. Verify: no duplicates, no gaps across all three phases
//
// Network name: your docker-compose network (deploy_kafka-net)

func TestNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	infra := NewTestInfra(t)
	defer infra.Close()

	roomID := fmt.Sprintf("eos-netpart-%s", uuid.New().String()[:8])
	defer infra.CleanupTestRoom(t, roomID)

	// Determin Docker network name
	// Your docker-compose.yml uses "kafka-net" which gets prefixed with the project direct name

	network := "deploy_kafka-net"
	startSeq := infra.GetSequenceForRoom(t, roomID)

	// Phase 1: Norma; operation
	batchSize := 3
	t.Logf("Phase 1: Sending %d messages (normal operation)", batchSize)
	ids1 := infra.ProduceBatch(t, roomID, batchSize)
	infra.WaitForMessages(t, roomID, batchSize, 30*time.Second)

	// Phase 2: Disconnect kafka3
	t.Log("Phase 2: Disconnecting kafka3 from network...")
	DockerNetworkDisconnect(t, network, "kafka3")
	time.Sleep(5 * time.Second) // Let cluster detect partition

	// Phase 3: Send messages during partition
	t.Logf("Phase 3: Sending %d messages during network partition", batchSize)
	ids2 := infra.ProduceBatch(t, roomID, batchSize)
	infra.WaitForMessages(t, roomID, batchSize*2, 30*time.Second)

	// Phase 4: Reconnect kafka3
	t.Log("Phase 4: Reconnecting kafka3 to network...")
	DockerNetworkConnect(t, network, "kafka3")
	time.Sleep(10 * time.Second) // Let broker rejoin ISR

	// Phase 5: Send messages after healing
	t.Logf("Phase 5: Sending %d messages after partition headled", batchSize)
	ids3 := infra.ProduceBatch(t, roomID, batchSize)

	// Wait for all messages
	totalMessages := batchSize * 3
	messages := infra.WaitForMessages(t, roomID, totalMessages, 60*time.Second)

	// ─── Exactly-Once Verification ───
	t.Log("Verifying exactly-once guarantees...")

	allIDs := append(append(ids1, ids2...), ids3...)
	VerifyAllMessageIDsPresent(t, allIDs, messages)
	VerifyNoDuplicateMesageIDs(t, messages)
	VerifyNoDuplicateSequences(t, messages)
	VerifySequenceContinuity(t, messages, startSeq, totalMessages)

	t.Logf("✅ Network partition test passed: %d messages, no duplicates, no gaps", len(messages))
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Idempotent Replay (Offset Reset)
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   1. Send a batch of messages and wait for them to be processed
//   2. Record the message count and sequence counter
//   3. Reset the aggregator consumer group offset to replay the messages
//   4. Restart the aggregator so it reprocesses from the reset offset
//   5. Wait and then verify: MongoDB should have the SAME data (no duplicates)
//      because the aggregator uses $setOnInsert for idempotent upserts
//
// This specifically tests the aggregator's idempotent MongoDB write —
// the persistMessage function that uses $setOnInsert keyed on message_id.

func TestIdempotentReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	infra := NewTestInfra(t)
	defer infra.Close()

	roomID := fmt.Sprintf("eos-replay-%s", uuid.New().String()[:8])
	defer infra.CleanupTestRoom(t, roomID)

	// Phase 1: Send and process messages
	batchSize := 5
	t.Logf("Phase 1: Sending %d messages", batchSize)
	producedIDs := infra.ProduceBatch(t, roomID, batchSize)
	messages := infra.WaitForMessages(t, roomID, batchSize, 30*time.Second)

	// Record state before replay
	messageCountBefore := len(messages)
	seqBefore := infra.GetSequenceForRoom(t, roomID)
	t.Logf("Before replay: %d messages, sequence=%d", messageCountBefore, seqBefore)

	// Phase 2: Stop the aggregator
	t.Log("Phase 2: Stopping aggregator...")
	DockerStop(t, "service-aggregator", 5)

	// Phase 3: Reset the aggregator consumer group offset to earlist
	// This forces the aggregator to re-read all messages from chat-timeline
	t.Log("Phase 3: Resetting aggregator consumer group effects...")
	resetCMD := []string{
		"/opt/kafka/bin/kafka-consumer-groups.sh",
		"--bootstrap-server", "kafka1:29092",
		"--group", "aggregator-group",
		"--topic", TopicChatTimeline,
		"--reset-offsets",
		"--to-earliest",
		"--execute",
	}
	output, err := DockerExec("kafka1", resetCMD...)
	if err != nil {
		t.Logf("Offset reset output: %s", output)
		t.Fatalf("failed to reset offsets: %v", err)
	}
	t.Logf("Offset reset result:\n%s", output)

	// Phase 4: Restart the aggregator - it will replay all the chat.Timeline messages
	t.Log("Phase 4: Restarting aggregator to trigger replay...")
	DockerStart(t, "service-aggregator")
	WaitForContainer(t, "service-aggregator", 30*time.Second)

	// Wait for replay to complete - the aggregator will reprocess everything
	// Give it extra time since its replaying from the beginning of the topic
	time.Sleep(20 * time.Second)

	// Phase 5: Verify idempotency
	t.Log("Phase 5: Verifying idempotency after replay...")
	messageAfter := infra.GetStoredMessages(t, roomID)

	// Check 1: same number of messages (no duplicates created)
	if len(messageAfter) != messageCountBefore {
		t.Errorf("DUPLICATE MESSAGES: had %d before relay. now have %d", messageCountBefore, len(messageAfter))
	}

	// Check 2: All original message IDs still present
	VerifyAllMessageIDsPresent(t, producedIDs, messageAfter)

	// Check 3: No duplicate message IDs
	VerifyNoDuplicateMesageIDs(t, messageAfter)

	// Check 4: No duplicate sequences
	VerifyNoDuplicateSequences(t, messageAfter)

	// Check 5: Each message_id should have exactly 1 document
	for _, id := range producedIDs {
		count := infra.CountMessageByID(t, id)
		if count != 1 {
			t.Errorf("message %s has %d documents (expected exactly 1)", id, count)
		}
	}

	t.Logf("✅ Idempotent replay test passed: %d messages before and after replay, no duplicates", len(messageAfter))
}
