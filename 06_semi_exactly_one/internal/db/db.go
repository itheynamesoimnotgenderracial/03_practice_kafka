package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRecord struct {
	ID             string
	IdempotencyKey string
	Status         string
	Payload        string
	ExAPIResponse  string
	FailedAtStep   string
	FailureReason  string
	RetryCount     int
}

type CustomerRow struct {
	ID         string
	Name       string
	Email      string
	ExternalID string
	CreatedAt  time.Time
}

// All consumer and API code depends only on this interface - nerver on
// concrete type. This enables: unit tests -> MockStore, integration -> PostgresStore
type Store interface {
	GetOutboxRecord(ctx context.Context, indempotencyKey string) (*OutboxRecord, error)
	CreateOutboxRecord(ctx context.Context, id, idempotencyKey, payload string) error
	UpdateOutboxStatus(ctx context.Context, idempotencyKey, status, extAPIResponse string) error
	MarkOutboxFailed(ctx context.Context, idempotencyKey, step, reason string) error
	SaveCustomerAndCompleteOutbox(ctx context.Context, customerID, name, email, externalID, idempotencyKey string) error
	IncrementRetryCount(ctx context.Context, idempotencyKey string) error
	Ping(ctx context.Context) error
	Close()
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	cfg.MinConns = 2
	cfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg pool: %w", err)
	}

	s := &PostgresStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration: %w", err)
	}
	return s, nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS outbox(
			id TEXT PRIMARY KEY,
			idempotency_key TEXT UNIQUE NOT NULL,
			status TEXT NOT NULL DEFAULT 'PENDING',
			payload TEXT,
			ext_api_response TEXT,
			retry_count INTEGER DEFAULT 0,
			failed_at_step TEXT,
			failure_reason TEXT,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS customers (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			external_id TEXT,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		);
	`)
	return err
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *PostgresStore) Close()                         { s.pool.Close() }
func (s *PostgresStore) GetOutboxRecord(ctx context.Context, key string) (*OutboxRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, status, payload,
			COALESCE(ext_api_response, ''), retry_count,
			COALESCE(failed_at_step, ''), COALESCE(failure_reason, '')
		FROM outbox WHERE idempotency_key=$1
	`, key)
	var r OutboxRecord
	if err := row.Scan(&r.ID, &r.IdempotencyKey, &r.Status, &r.Payload, &r.ExAPIResponse, &r.RetryCount, &r.FailedAtStep, &r.FailureReason); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getOutboxRecord: %w", err)
	}
	return &r, nil
}

func (s *PostgresStore) CreateOutboxRecord(ctx context.Context, id, key, payload string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO outbox (id, idempotency_key, status, payload) VALUES ($1,$2,'PENDING',$3)`, id, key, payload)
	return err
}

func (s *PostgresStore) MarkOutboxFailed(ctx context.Context, key, step, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET status='FAILED', failed_at_step=$1, failure_reason=$2,
		retry_count=retry_count+1, upload_at=NOW()
		WHERE idempotency_key=$3
	`, step, reason, key)
	return err
}

// SaveCustomerAndCompleteOutbox atomically upserts the customer row and sets
// the outbox status to COMPLETED inside a single DB transaction.
func (s *PostgresStore) SaveCustomerAndCompleteOutbox(ctx context.Context, customerID, name, email, externalID, key string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO customers (id, name, email, external_id, updated_at)
		VALUES ($1,$2,$3,$4,NOW())
		ON CONFLICT (id) DO UPDATE
		SET name=EXCLUDED.name, email=EXCLUDED.email,
			external_id=EXCLUDED.external_id, updated_at=NOW()
	`, customerID, name, email, externalID)
	if err != nil {
		return fmt.Errorf("upsert customer: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE outbox SET status='COMPLETED', updated_at=NOW() WHERE idempotency_key=$1
	`, key)
	if err != nil {
		return fmt.Errorf("complete outbox: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) IncrementRetryCount(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET retry_count=retry_count+1, updated_at=NOW()
		WHERE idempotency_key=$1
	`, key)
	return err
}

func (s *PostgresStore) UpdateOutboxStatus(ctx context.Context, key, status, extResp string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox
			SET status=$1,
				ext_api_response=CASE WHEN $2='' THEN ext_api_response ELSE $2 END,
				updated_at=NOW()
			WHERE idempotency_key=$3
	`, status, extResp, key)
	return err
}
