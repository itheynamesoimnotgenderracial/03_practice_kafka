package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"simple-semo-eos/external"
	"simple-semo-eos/internal/db"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
	"simple-semo-eos/internal/serde"
	"time"
)

const maxRetries = 3

// CustomerConfig holds every dependency the customer consumer needs. All fields are interfaces so tests can inject mocks.
type CustomerConfig struct {
	Consumer      kafka.Consumer
	Producer      kafka.Producer
	Store         db.Store
	Serde         serde.Codec
	CustomerTopic string
	DLQTopic      string
	ExternalAPI   external.CallerFunc
}

// RunCustomerConsumer is the main processing loop.
//
// Step 1 -> build idempotency key
// Step 2 -> check outbox: COMPLETED=skip, EXT_API_DONE=resume DB save, PENDING=retry ext API
// Step 3 -> write PENDING to outbox BEFORE calling anything external
// Step 4 -> call external API with retry + exponential backoff
// Step 5 -> write EXT_API_DONE checkpoint immediately after success
// Step 6 -> atomic DB transaction: upsert customer + outbox=COMPLETED
// Step 7 -> commit Kafka offset LAST, only after everything succeeds
func RunCustomerConsumer(ctx context.Context, cfg CustomerConfig) error {
	if err := cfg.Consumer.Subscribe([]string{cfg.CustomerTopic}); err != nil {
		return err
	}
	log.Printf("[Customer] subscribed to %s", cfg.CustomerTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Customer] shutting down")
			return cfg.Consumer.Close()
		default:
		}

		kmsg, err := cfg.Consumer.Poll(200)
		if err != nil {
			log.Printf("[Customer] poll over: %v", err)
			continue
		}
		if kmsg == nil {
			continue
		}

		// Deserialize Confluent Avro wire format → CustomerMessage
		var msg models.CustomerMessage
		if err := cfg.Serde.Deserialize(kmsg.Value, &msg); err != nil {
			sendToDLQ(ctx, cfg.Producer, cfg.Serde, cfg.DLQTopic, models.DLQMessage{
				FailedAtStep: "DESERIALIZATION",
				Reason:       err.Error(),
			})
			if commitErr := cfg.Consumer.CommitMessage(kmsg); commitErr != nil {
				log.Printf("[Customer] x commit after DLQ deserialization error: %v", commitErr)
			}
			continue
		}

		// Re-marshal to JSON for outbox storage (human-readable, avoids binary in TEXT column)
		rawJSON, _ := json.Marshal(msg)

		if err := processWithIdempotency(ctx, cfg, msg, string(rawJSON)); err != nil {
			log.Printf("[Customer] x unhandled error message_id=%s: %v", msg.MessageID, err)
			// Do not commit - redeliver so idempotency key handles it on retry
			continue
		}

		// Step 7: commit offset only after everything succeeded
		if err := cfg.Consumer.CommitMessage(kmsg); err != nil {
			log.Printf("[Customer] x commit failed: %v", err)
		}
	}
}

// processWithIdempotency is the testable core - a pure function that can be called directly in unit tests without up the Kafka consumer loop.
func processWithIdempotency(ctx context.Context, cfg CustomerConfig, msg models.CustomerMessage, raw string) error {
	key := BuildIdempotencyKey(msg)
	log.Printf("[Customer] processing | message_id=%s key=%s", msg.MessageID, key)

	outbox, err := cfg.Store.GetOutboxRecord(ctx, key)
	if err != nil {
		return fmt.Errorf("outbox lookup: %w", err)
	}

	if outbox != nil {
		switch outbox.Status {
		case "COMPLETED":
			// Duplicate Kafka delivery - already done, safe to skip
			log.Printf("[Customer] ⟳ duplicate, skipping | key=%s", key)
			return nil
		case "EXT_API_DONE":
			// Crash after ext API call but before DB save - skip the ext call, use the stored response to finish the DB save.
			// Guard against an infinite crash loop: if the process keeps crashing during saveToDatabase,
			// retry_count acts as a crash counter. Once it exceeds maxRetries, bail to DLQ.
			if outbox.RetryCount >= maxRetries {
				log.Printf("[Customer] x EXT_API_DONE crash loop detected, sending to DLQ | key=%s retries=%d", key, outbox.RetryCount)
				cfg.Store.MarkOutboxFailed(ctx, key, "EXT_API_DONE_CRASH_LOOP", "exceeded retry limit on DB save")
				sendToDLQ(ctx, cfg.Producer, cfg.Serde, cfg.DLQTopic, models.DLQMessage{
					OriginalPayload: msg,
					FailedAtStep:    "EXT_API_DONE_CRASH_LOOP",
					Reason:          "exceeded retry limit on DB save",
					IdempotencyKey:  key,
					RetryCount:      outbox.RetryCount,
				})
				return nil
			}
			log.Printf("[Customer] ⟳ resuming from EXT_API_DONE | key=%s attempt=%d/%d", key, outbox.RetryCount+1, maxRetries)
			cfg.Store.IncrementRetryCount(ctx, key)
			return saveToDatabase(ctx, cfg, msg, outbox.ExAPIResponse, key)
		case "PENDING":
			// Crashed before ext API call - fall through to call it now
			log.Printf("[Customer] ⟳ resuming from PENDING | key=%s", key)
		case "FAILED":
			// DLQ replay - reset to PENDING and retry
			log.Printf("[Customer] ⟳ replaying FAILED message | key=%s", key)
			_ = cfg.Store.UpdateOutboxStatus(ctx, key, "PENDING", "")
		}
	} else {
		// ── Step 3: first time - write PENDING before anything else ──────
		// This is the "I've started" marker. Without it, a crash between here
		// and the ext API call is invisible and eats retry budget.
		if err := cfg.Store.CreateOutboxRecord(ctx, newUUID(), key, raw); err != nil {
			return fmt.Errorf("create outbox: %w", err)
		}
		log.Printf("[Customer] ✎ PENDING outbox created | key=%s", key)
		outbox = &db.OutboxRecord{IdempotencyKey: key, Status: "PENDING"}
	}

	// ── Step 4: call external API with retry + exponential backoff ────────────
	extResp, err := callExternalWithRetry(ctx, cfg, msg, key, outbox.RetryCount)
	if err != nil {
		cfg.Store.MarkOutboxFailed(ctx, key, "EXTERNAL_API", err.Error())
		sendToDLQ(ctx, cfg.Producer, cfg.Serde, cfg.DLQTopic, models.DLQMessage{
			OriginalPayload: msg,
			FailedAtStep:    "EXTERNAL_API",
			Reason:          err.Error(),
			IdempotencyKey:  key,
			RetryCount:      outbox.RetryCount,
		})
		return nil
	}

	// ── Step 5: persist EXT_API_DONE checkpoint ────────────
	// If the process crashes NOW, the next retry sees the EXT_API_DONE and skips
	// the ext API call - preventing a duplicate call to a non-rollback-able API
	extJSON, _ := json.Marshal(extResp)
	if err := cfg.Store.UpdateOutboxStatus(ctx, key, "EXT_API_DONE", string(extJSON)); err != nil {
		return fmt.Errorf("persist EXT_API_DONE: %w", err)
	}
	log.Printf("[Customer] ✓ EXT_API_DONE | ext_id=%s", extResp.ExternalID)

	// ── Step 6: atomic DB save ────────────────────────────────────────────────
	return saveToDatabase(ctx, cfg, msg, string(extJSON), key)
}

// callExternalWithRetry retries with exponential backoff: 0s -> 1s -> 4s
func callExternalWithRetry(ctx context.Context, cfg CustomerConfig, msg models.CustomerMessage, key string, _ int) (*models.ExternalAPIResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * time.Second
			log.Printf("[Customer] ↻ retry %d/%d backoff=%s key=%s", attempt+1, maxRetries, backoff, key)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during backoff")
			case <-time.After(backoff):
			}
		}
		resp, err := cfg.ExternalAPI(msg)
		if err == nil {
			if attempt > 0 {
				log.Printf("[Customer] ✓ ext API succeeded on attempt %d", attempt+1)
			}
			return resp, nil
		}
		lastErr = err
		log.Printf("[Customer] x ext API failed after %d attempts: %s", attempt+1, err)
	}
	return nil, fmt.Errorf("ext API failed after %d attempts: %w", maxRetries, lastErr)
}

// saveToDatabase atomically upserts the customer and marks outbox=COMPLETED
func saveToDatabase(ctx context.Context, cfg CustomerConfig, msg models.CustomerMessage, extJSON, key string) error {
	var extResp models.ExternalAPIResponse
	if err := json.Unmarshal([]byte(extJSON), &extResp); err != nil {
		return fmt.Errorf("parse ext response: %w", err)
	}
	log.Printf("[Customer] ✎ saving customer | customer_id=%s ext_id=%s", msg.CustomerID, extResp.ExternalID)

	if err := cfg.Store.SaveCustomerAndCompleteOutbox(ctx, msg.CustomerID, msg.Name, msg.Email, extResp.ExternalID, key); err != nil {
		_ = cfg.Store.MarkOutboxFailed(ctx, key, "DB_SAVE", err.Error())
		sendToDLQ(ctx, cfg.Producer, cfg.Serde, cfg.DLQTopic, models.DLQMessage{
			OriginalPayload: msg,
			FailedAtStep:    "DB_SAVE",
			Reason:          err.Error(),
			IdempotencyKey:  key,
		})
		return nil
	}
	log.Printf("[Customer] ✓ COMPLETED | customer_id=%s key=%s", msg.CustomerID, key)
	return nil
}
