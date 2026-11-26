package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/akhmed9505/image-processor/internal/service/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidOutboxInput    = errors.New("invalid outbox repository input")
	ErrOutboxClaimFenced     = outbox.ErrClaimFenced
	ErrOutboxCommitUncertain = outbox.ErrCommitUncertain
)

// OutboxEvent is the publisher's leased notification.
type OutboxEvent = outbox.Event

// OutboxRepository owns publication transactions; callers own pool.
type OutboxRepository struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
}

// NewOutboxRepository validates the lease used for each publication claim.
func NewOutboxRepository(pool *pgxpool.Pool, leaseDuration time.Duration) (*OutboxRepository, error) {
	if leaseDuration < time.Microsecond {
		return nil, ErrInvalidOutboxInput
	}
	return &OutboxRepository{pool: pool, leaseDuration: leaseDuration}, nil
}

// Claim returns at most limit events in unspecified order, replacing expired claims.
// Publish only after a nil error; uncertain COMMIT returns no events and claims expire.
func (r *OutboxRepository) Claim(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit < 1 || limit > math.MaxInt32 {
		return nil, ErrInvalidOutboxInput
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text FROM outbox
 WHERE published_at IS NULL AND available_at<=clock_timestamp()
 AND (lease_until IS NULL OR lease_until<=clock_timestamp())
 ORDER BY available_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("lock available outbox events: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read locked outbox IDs: %w", err)
	}
	// Start leases after acquiring all row locks.
	rows, err = tx.Query(ctx, `UPDATE outbox SET claim_token=gen_random_uuid(),
 lease_until=clock_timestamp()+$2::bigint*interval '1 microsecond',publish_attempts=publish_attempts+1
 WHERE id=ANY($1::uuid[]) AND published_at IS NULL AND available_at<=clock_timestamp()
 AND (lease_until IS NULL OR lease_until<=clock_timestamp())
 RETURNING id::text,event_type,topic,message_key,payload,claim_token::text,lease_until,publish_attempts`, ids, r.leaseDuration.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutboxEvent, error) {
		var event OutboxEvent
		err := row.Scan(&event.ID, &event.EventType, &event.Topic, &event.MessageKey, &event.Payload, &event.ClaimToken, &event.LeaseUntil, &event.PublishAttempts)
		return event, err
	})
	if err != nil {
		return nil, fmt.Errorf("read claimed outbox events: %w", err)
	}
	if err := commitOutbox(ctx, tx); err != nil {
		return nil, err
	}
	return events, nil
}

// MarkPublished records successful publication only for the current live claim.
func (r *OutboxRepository) MarkPublished(ctx context.Context, id, token string) error {
	return r.finishClaim(ctx, id, token, "", nil)
}

// Reschedule records a safe error code and releases the claim until availableAt.
// The caller supplies a future retry time and computes any backoff or jitter.
func (r *OutboxRepository) Reschedule(ctx context.Context, id, token, errorCode string, availableAt time.Time) error {
	if errorCode == "" || availableAt.IsZero() {
		return ErrInvalidOutboxInput
	}
	return r.finishClaim(ctx, id, token, errorCode, &availableAt)
}

func (r *OutboxRepository) finishClaim(ctx context.Context, id, token, errorCode string, availableAt *time.Time) error {
	parsed, err := uuid.Parse(token)
	if err != nil {
		return ErrOutboxClaimFenced
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin outbox finalization: %w", err)
	}
	defer tx.Rollback(ctx)
	var live bool
	if err := tx.QueryRow(ctx, "SELECT id::text FROM outbox WHERE id=$1 FOR UPDATE", id).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOutboxClaimFenced
		}
		return fmt.Errorf("lock outbox event: %w", err)
	}
	// Check current time after lock waits.
	if err := tx.QueryRow(ctx, `SELECT COALESCE(published_at IS NULL AND claim_token=$2 AND lease_until>clock_timestamp(),false)
 FROM outbox WHERE id=$1`, id, parsed.String()).Scan(&live); err != nil {
		return fmt.Errorf("check outbox claim: %w", err)
	}
	if !live {
		return ErrOutboxClaimFenced
	}
	if availableAt != nil {
		var future bool
		if err := tx.QueryRow(ctx, "SELECT $1::timestamptz>clock_timestamp()", *availableAt).Scan(&future); err != nil {
			return fmt.Errorf("check outbox retry time: %w", err)
		}
		if !future {
			return ErrInvalidOutboxInput
		}
	}
	result, err := tx.Exec(ctx, `UPDATE outbox SET
 published_at=CASE WHEN $3::timestamptz IS NULL THEN clock_timestamp() END,
 available_at=COALESCE($3,available_at),last_error_code=NULLIF($4,''),claim_token=NULL,lease_until=NULL
 WHERE id=$1 AND claim_token=$2 AND published_at IS NULL AND lease_until>clock_timestamp()`, id, parsed.String(), availableAt, errorCode)
	if err != nil {
		return fmt.Errorf("finalize outbox event: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrOutboxClaimFenced
	}
	return commitOutbox(ctx, tx)
}

func commitOutbox(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("outbox rolled back on commit: %w", err)
		}
		return fmt.Errorf("%w: %w", ErrOutboxCommitUncertain, err)
	}
	return nil
}
