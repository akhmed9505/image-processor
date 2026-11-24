package postgres

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/jackc/pgx/v5"
)

// RecoveryRepository restores jobs using the JobRepository policy and caller-owned pool.
type RecoveryRepository struct{ jobs *JobRepository }

// NewRecoveryRepository reuses job attempt limits, topics, and redispatch policy.
func NewRecoveryRepository(jobs *JobRepository) *RecoveryRepository {
	return &RecoveryRepository{jobs: jobs}
}

// Recover restores at most limit expired attempts; errors return no IDs, even on uncertain COMMIT.
// retryDelay runs under locks without I/O and must return at least one microsecond.
// It is not called for exhausted jobs and may be nil if none need retrying.
func (r *RecoveryRepository) Recover(ctx context.Context, limit int, retryDelay func(attempts int) time.Duration) ([]string, error) {
	return r.run(ctx, limit, true, retryDelay)
}

// Redispatch advances at most limit due jobs, preserving existing unpublished requests.
// ErrJobCommitUncertain means the batch may have committed.
func (r *RecoveryRepository) Redispatch(ctx context.Context, limit int) ([]string, error) {
	return r.run(ctx, limit, false, nil)
}

func (r *RecoveryRepository) run(ctx context.Context, limit int, recovering bool, retryDelay func(int) time.Duration) ([]string, error) {
	if limit < 1 || limit > math.MaxInt32 {
		return nil, ErrInvalidJobInput
	}
	tx, err := r.jobs.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin recovery: %w", err)
	}
	defer tx.Rollback(ctx)
	predicate := `status IN ('pending','retrying') AND next_dispatch_at<=clock_timestamp() AND (next_retry_at IS NULL OR next_retry_at<=clock_timestamp())`
	if recovering {
		predicate = `status='processing' AND lease_until<=clock_timestamp()`
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM images WHERE deleted_at IS NULL AND EXISTS
 (SELECT 1 FROM jobs WHERE image_id=images.id AND `+predicate+`) ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("lock recovery images: %w", err)
	}
	images, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read recovery images: %w", err)
	}
	rows, err = tx.Query(ctx, "SELECT "+jobColumns+" FROM jobs WHERE image_id=ANY($1::uuid[]) AND "+predicate+" ORDER BY image_id,id LIMIT $2 FOR UPDATE SKIP LOCKED", images, limit)
	if err != nil {
		return nil, fmt.Errorf("lock recovery jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) { return scanJob(row) })
	if err != nil {
		return nil, fmt.Errorf("read recovery jobs: %w", err)
	}
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.ID)
	}
	if recovering {
		if _, err := tx.Exec(ctx, "SELECT object_key FROM storage_objects WHERE job_id=ANY($1::uuid[]) ORDER BY object_key FOR UPDATE", ids); err != nil {
			return nil, fmt.Errorf("lock recovery objects: %w", err)
		}
	}
	now, err := jobClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	failure := JobFailure{Code: "lease_expired", Message: "processing lease expired"}
	for _, job := range jobs {
		var retryAt *time.Time
		if recovering {
			if job.LeaseUntil.After(now) {
				return nil, ErrAttemptFenced
			}
			result, err := tx.Exec(ctx, `UPDATE storage_objects SET state='discarded',updated_at=$3
 WHERE job_id=$1 AND attempt_token=$2 AND image_id=$4 AND state IN ('writing','discarded')`, job.ID, job.AttemptToken, now, job.ImageID)
			if err != nil {
				return nil, fmt.Errorf("discard expired result intent: %w", err)
			}
			if result.RowsAffected() != 1 {
				return nil, ErrInvalidStorageTransition
			}
			if job.Attempts >= r.jobs.options.MaxAttempts {
				if _, err := r.jobs.failJob(ctx, tx, job, failure, now); err != nil {
					return nil, fmt.Errorf("fail recovered job: %w", err)
				}
				continue
			}
			if retryDelay == nil {
				return nil, ErrInvalidJobInput
			}
			delay := retryDelay(job.Attempts)
			if delay < time.Microsecond {
				return nil, ErrInvalidJobInput
			}
			at := now.Add(delay)
			retryAt = &at
			if _, err := tx.Exec(ctx, `UPDATE jobs SET status='retrying',attempt_token=NULL,lease_until=NULL,next_retry_at=$2,
 next_dispatch_at=$2::timestamptz+$3::bigint*interval '1 microsecond',last_error_code=$4,last_error=$5,updated_at=$6
 WHERE id=$1`, job.ID, *retryAt, r.jobs.options.RedispatchInterval.Microseconds(), failure.Code, failure.Message, now); err != nil {
				return nil, fmt.Errorf("retry recovered job: %w", err)
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE jobs SET next_dispatch_at=$2::timestamptz+$3::bigint*interval '1 microsecond' WHERE id=$1`, job.ID, now, r.jobs.options.RedispatchInterval.Microseconds()); err != nil {
				return nil, fmt.Errorf("advance redispatch: %w", err)
			}
		}
		var enqueueErr error
		if recovering {
			enqueueErr = enqueueJobRequest(ctx, tx, job.ID, r.jobs.options.JobsTopic, retryAt)
		} else {
			enqueueErr = enqueueMissingJobRequest(ctx, tx, job.ID, r.jobs.options.JobsTopic)
		}
		if enqueueErr != nil {
			return nil, enqueueErr
		}
	}
	if err := commitJob(ctx, tx); err != nil {
		return nil, err
	}
	return ids, nil
}
