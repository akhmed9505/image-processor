package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrDeleteCommitUncertain requires checking current state or repeating Delete before cleanup.
var ErrDeleteCommitUncertain = errors.New("delete commit outcome is uncertain")

// DeleteRepository owns transactions for logical image deletion; callers own pool.
type DeleteRepository struct{ pool *pgxpool.Pool }

// NewDeleteRepository uses pool without taking ownership of its lifecycle.
func NewDeleteRepository(pool *pgxpool.Pool) *DeleteRepository {
	return &DeleteRepository{pool: pool}
}

// Delete tombstones an image and cancels unfinished jobs, preserving object records.
// Repeated calls preserve cleanup state; missing images return domain.ErrImageNotFound.
func (r *DeleteRepository) Delete(ctx context.Context, id string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin image deletion: %w", err)
	}
	defer tx.Rollback(ctx)
	var deleted pgtype.Timestamptz
	if err := tx.QueryRow(ctx, "SELECT deleted_at FROM images WHERE id=$1 FOR UPDATE", id).Scan(&deleted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrImageNotFound
		}
		return fmt.Errorf("lock deleted image: %w", err)
	}
	if deleted.Valid {
		return nil
	}
	if _, err := tx.Exec(ctx, "SELECT id FROM jobs WHERE image_id=$1 ORDER BY id FOR UPDATE", id); err != nil {
		return fmt.Errorf("lock image jobs for deletion: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT object_key FROM storage_objects WHERE image_id=$1 ORDER BY object_key FOR UPDATE", id); err != nil {
		return fmt.Errorf("lock image objects for deletion: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return fmt.Errorf("read deletion time: %w", err)
	}
	// Non-retained intents may have PUTs in flight after cancellation; preserve their deadlines.
	if _, err := tx.Exec(ctx, `UPDATE images SET deleted_at=$2,cleanup_after=GREATEST($2::timestamptz,
 (SELECT max(write_until) FROM storage_objects WHERE image_id=$1 AND state<>'retained')) WHERE id=$1`, id, now); err != nil {
		return fmt.Errorf("tombstone image: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status='cancelled',attempt_token=NULL,lease_until=NULL,
 next_retry_at=NULL,next_dispatch_at=NULL,completed_at=$2,updated_at=$2
 WHERE image_id=$1 AND status IN ('pending','processing','retrying')`, id, now); err != nil {
		return fmt.Errorf("cancel image jobs: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE storage_objects SET state='discarded',updated_at=$2
 WHERE image_id=$1 AND state<>'discarded'`, id, now); err != nil {
		return fmt.Errorf("discard image objects: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE job_id IN (SELECT id FROM jobs WHERE image_id=$1)
 AND event_type='image.processing.requested' AND published_at IS NULL`, id); err != nil {
		return fmt.Errorf("remove obsolete image requests: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("image deletion rolled back on commit: %w", err)
		}
		return fmt.Errorf("%w: %w", ErrDeleteCommitUncertain, err)
	}
	return nil
}
