package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidCleanupInput    = errors.New("invalid cleanup repository input")
	ErrCleanupFenced          = errors.New("cleanup claim no longer owns a live lease")
	ErrCleanupCommitUncertain = errors.New("cleanup commit outcome is uncertain")
)

// CleanupClaim grants temporary ownership of a deleted image cleanup pass.
type CleanupClaim struct {
	ImageID, Token string
	LeaseUntil     time.Time
}

// CleanupRepository coordinates cleanup without deleting registry or image rows.
type CleanupRepository struct {
	pool  *pgxpool.Pool
	lease time.Duration
}

// NewCleanupRepository validates the lease used for deleted image cleanup.
func NewCleanupRepository(pool *pgxpool.Pool, lease time.Duration) (*CleanupRepository, error) {
	if lease < time.Microsecond {
		return nil, ErrInvalidCleanupInput
	}
	return &CleanupRepository{pool, lease}, nil
}

// Claim acquires at most limit due tombstones, skipping concurrent cleanup locks.
// An uncertain COMMIT returns no claims; any committed leases can expire normally.
func (r *CleanupRepository) Claim(ctx context.Context, limit int) ([]CleanupClaim, error) {
	if limit < 1 || limit > math.MaxInt32 {
		return nil, ErrInvalidCleanupInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin cleanup claim: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text FROM images WHERE deleted_at IS NOT NULL AND cleanup_after<=clock_timestamp()
 AND (cleanup_lease_until IS NULL OR cleanup_lease_until<=clock_timestamp()) ORDER BY cleanup_after,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("lock cleanup images: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, `UPDATE images SET cleanup_token=gen_random_uuid(),cleanup_lease_until=clock_timestamp()+$2::bigint*interval '1 microsecond'
 WHERE id=ANY($1::uuid[]) AND deleted_at IS NOT NULL AND cleanup_after<=clock_timestamp()
 AND (cleanup_lease_until IS NULL OR cleanup_lease_until<=clock_timestamp()) RETURNING id::text,cleanup_token::text,cleanup_lease_until`, ids, r.lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim cleanup images: %w", err)
	}
	claims, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (CleanupClaim, error) {
		var c CleanupClaim
		err := row.Scan(&c.ImageID, &c.Token, &c.LeaseUntil)
		return c, err
	})
	if err != nil {
		return nil, err
	}
	if err := commitCleanup(ctx, tx); err != nil {
		return nil, err
	}
	return claims, nil
}

// Complete schedules another pass for late PUTs, retaining tombstones and registry keys.
func (r *CleanupRepository) Complete(ctx context.Context, id, token string, nextAt time.Time) error {
	return r.finish(ctx, id, token, nextAt, true)
}

// Reschedule releases a failed pass without changing last_cleanup_at.
func (r *CleanupRepository) Reschedule(ctx context.Context, id, token string, nextAt time.Time) error {
	return r.finish(ctx, id, token, nextAt, false)
}

func (r *CleanupRepository) finish(ctx context.Context, id, token string, nextAt time.Time, completed bool) error {
	if nextAt.IsZero() {
		return ErrInvalidCleanupInput
	}
	parsed, err := uuid.Parse(token)
	if err != nil {
		return ErrCleanupFenced
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cleanup finalization: %w", err)
	}
	defer tx.Rollback(ctx)
	var locked string
	if err := tx.QueryRow(ctx, "SELECT id::text FROM images WHERE id=$1 FOR UPDATE", id).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrCleanupFenced
		}
		return fmt.Errorf("lock cleanup image: %w", err)
	}
	var live, future bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(deleted_at IS NOT NULL AND cleanup_token=$2 AND cleanup_lease_until>clock_timestamp(),false),$3::timestamptz>clock_timestamp() FROM images WHERE id=$1`, id, parsed.String(), nextAt).Scan(&live, &future); err != nil {
		return fmt.Errorf("check cleanup claim: %w", err)
	}
	if !live {
		return ErrCleanupFenced
	}
	if !future {
		return ErrInvalidCleanupInput
	}
	result, err := tx.Exec(ctx, `UPDATE images SET cleanup_after=$3,cleanup_token=NULL,cleanup_lease_until=NULL,
 last_cleanup_at=CASE WHEN $4 THEN clock_timestamp() ELSE last_cleanup_at END
 WHERE id=$1 AND cleanup_token=$2 AND cleanup_lease_until>clock_timestamp()`, id, parsed.String(), nextAt, completed)
	if err != nil {
		return fmt.Errorf("finish cleanup pass: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrCleanupFenced
	}
	return commitCleanup(ctx, tx)
}

// CleanupPage separates objects authorized for deletion from scan progress.
// An empty NextCursor ends the pass; Objects may be empty before that point.
type CleanupPage struct {
	Objects    []StorageObject
	NextCursor string
}

// FindDeletable discards at most limit objects past write_until+grace in any state,
// protecting active references and leases. Records remain discoverable for late PUTs.
// Start with afterKey=""; NextCursor advances in C order even when Objects is empty.
// Uncertain COMMIT returns no objects or cursor and does not authorize deletion.
func (r *CleanupRepository) FindDeletable(ctx context.Context, limit int, afterKey string, grace time.Duration) (CleanupPage, error) {
	if limit < 1 || limit > math.MaxInt32 || grace < 0 {
		return CleanupPage{}, ErrInvalidCleanupInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return CleanupPage{}, fmt.Errorf("begin object cleanup: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT object_key FROM storage_objects WHERE object_key COLLATE "C">$3 COLLATE "C" AND write_until+$2::bigint*interval '1 microsecond'<=clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM images i WHERE i.deleted_at IS NULL AND (i.original_key=storage_objects.object_key OR EXISTS
 (SELECT 1 FROM jobs j WHERE j.image_id=i.id AND j.status='completed' AND j.result_key=storage_objects.object_key)))
 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=storage_objects.job_id AND j.status='processing' AND j.attempt_token=storage_objects.attempt_token AND j.lease_until>clock_timestamp())
 ORDER BY object_key COLLATE "C" LIMIT $1`, limit, grace.Microseconds(), afterKey)
	if err != nil {
		return CleanupPage{}, fmt.Errorf("find cleanup candidates: %w", err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return CleanupPage{}, err
	}
	// Discarding an expired original intent prevents upload finalization, even without an image.
	if _, err := tx.Exec(ctx, `SELECT id FROM images WHERE id IN (SELECT image_id FROM storage_objects WHERE object_key=ANY($1::text[])) ORDER BY id FOR UPDATE`, keys); err != nil {
		return CleanupPage{}, fmt.Errorf("lock object owners: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM jobs WHERE image_id IN (SELECT image_id FROM storage_objects WHERE object_key=ANY($1::text[])) ORDER BY image_id,id FOR UPDATE`, keys); err != nil {
		return CleanupPage{}, fmt.Errorf("lock cleanup jobs: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT object_key FROM storage_objects WHERE object_key=ANY($1::text[]) ORDER BY object_key FOR UPDATE", keys); err != nil {
		return CleanupPage{}, fmt.Errorf("lock cleanup objects: %w", err)
	}
	rows, err = tx.Query(ctx, `UPDATE storage_objects s SET state='discarded',updated_at=clock_timestamp()
 WHERE object_key=ANY($1::text[]) AND write_until+$2::bigint*interval '1 microsecond'<=clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM images i WHERE i.deleted_at IS NULL AND (i.original_key=s.object_key OR EXISTS
 (SELECT 1 FROM jobs j WHERE j.image_id=i.id AND j.status='completed' AND j.result_key=s.object_key)))
 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=s.job_id AND j.status='processing' AND j.attempt_token=s.attempt_token AND j.lease_until>clock_timestamp())
 RETURNING `+storageColumns, keys, grace.Microseconds())
	if err != nil {
		return CleanupPage{}, fmt.Errorf("select safe cleanup objects: %w", err)
	}
	objects, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (StorageObject, error) { return scanStorageObject(row) })
	if err != nil {
		return CleanupPage{}, err
	}
	if err := commitCleanup(ctx, tx); err != nil {
		return CleanupPage{}, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].ObjectKey < objects[j].ObjectKey })
	page := CleanupPage{Objects: objects}
	if len(keys) > 0 {
		page.NextCursor = keys[len(keys)-1]
	}
	return page, nil
}

func commitCleanup(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("cleanup rolled back on commit: %w", err)
		}
		return fmt.Errorf("%w: %w", ErrCleanupCommitUncertain, err)
	}
	return nil
}
