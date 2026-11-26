package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrJobNotFound        = errors.New("job not found")
	ErrInvalidJobInput    = errors.New("invalid job repository input")
	ErrAttemptFenced      = processor.ErrAttemptFenced
	ErrJobNeedsRecovery   = errors.New("expired processing job requires recovery")
	ErrJobCommitUncertain = processor.ErrJobCommitUncertain
)

// ClaimDisposition describes a durable admission outcome.
type ClaimDisposition = processor.ClaimDisposition

// Only ClaimAcquired grants permission to run the returned attempt.
const (
	ClaimAcquired     = processor.ClaimAcquired
	ClaimProcessing   = processor.ClaimProcessing
	ClaimWaitingRetry = processor.ClaimWaitingRetry
	ClaimTerminal     = processor.ClaimTerminal
	ClaimImageDeleted = processor.ClaimImageDeleted
	ClaimExhausted    = processor.ClaimExhausted
)

// ClaimResult contains the service's admission data.
type ClaimResult = processor.ClaimResult

// JobRepositoryOptions controls leases, attempt limits, and durable notifications.
type JobRepositoryOptions struct {
	JobsTopic, DLQTopic               string
	MaxAttempts                       int
	LeaseDuration, RedispatchInterval time.Duration
}

// JobRepository owns transactions for processing attempts; callers own pool.
type JobRepository struct {
	pool    *pgxpool.Pool
	options JobRepositoryOptions
}

// NewJobRepository validates settings used by claim and attempt finalization.
func NewJobRepository(pool *pgxpool.Pool, options JobRepositoryOptions) (*JobRepository, error) {
	if options.JobsTopic == "" || options.DLQTopic == "" || options.JobsTopic == options.DLQTopic || options.MaxAttempts < 1 || options.MaxAttempts > math.MaxInt32 || options.LeaseDuration < time.Microsecond || options.RedispatchInterval < time.Microsecond {
		return nil, ErrInvalidJobInput
	}
	return &JobRepository{pool: pool, options: options}, nil
}

type lockedJob struct {
	job         domain.Job
	contentType string
	originalKey string
	deleted     bool
}

func lockJob(ctx context.Context, tx pgx.Tx, id string) (lockedJob, error) {
	var imageID string
	if err := tx.QueryRow(ctx, "SELECT image_id::text FROM jobs WHERE id=$1", id).Scan(&imageID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return lockedJob{}, ErrJobNotFound
		}
		return lockedJob{}, fmt.Errorf("find job parent: %w", err)
	}
	var deleted pgtype.Timestamptz
	var contentType, originalKey string
	if err := tx.QueryRow(ctx, "SELECT deleted_at,content_type,original_key FROM images WHERE id=$1 FOR UPDATE", imageID).Scan(&deleted, &contentType, &originalKey); err != nil {
		return lockedJob{}, fmt.Errorf("lock job image: %w", err)
	}
	job, err := scanJob(tx.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id=$1 AND image_id=$2 FOR UPDATE", id, imageID))
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedJob{}, ErrJobNotFound
	}
	if err != nil {
		return lockedJob{}, fmt.Errorf("lock job: %w", err)
	}
	return lockedJob{job: job, contentType: contentType, originalKey: originalKey, deleted: deleted.Valid}, nil
}

func jobClock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read PostgreSQL time: %w", err)
	}
	return now, nil
}

func commitJob(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("job rolled back on commit: %w", err)
		}
		return fmt.Errorf("%w: %w", ErrJobCommitUncertain, err)
	}
	return nil
}

func resultObjectKey(job domain.Job, contentType string) string {
	ext := "png"
	if contentType == "image/jpeg" {
		ext = "jpg"
	}
	return fmt.Sprintf("%s/results/%s/%s.%s", job.ImageID, job.ID, job.AttemptToken, ext)
}

// Claim creates a leased attempt and intent; reserve a worker slot first.
// Expired attempts require recovery. Errors, including uncertain COMMIT, must not be acknowledged.
func (r *JobRepository) Claim(ctx context.Context, id string) (ClaimResult, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ClaimResult{}, fmt.Errorf("begin job claim: %w", err)
	}
	defer tx.Rollback(ctx)
	locked, err := lockJob(ctx, tx, id)
	if err != nil {
		return ClaimResult{}, err
	}
	job := locked.job
	now, err := jobClock(ctx, tx)
	if err != nil {
		return ClaimResult{}, err
	}
	var result ClaimResult
	switch {
	case locked.deleted:
		result.Disposition = ClaimImageDeleted
	case job.Status.IsTerminal():
		result.Disposition = ClaimTerminal
	case job.Status == domain.JobStatusProcessing:
		if !job.LeaseUntil.After(now) {
			return ClaimResult{}, ErrJobNeedsRecovery
		}
		result.Disposition = ClaimProcessing
	case job.Status != domain.JobStatusPending && job.Status != domain.JobStatusRetrying:
		return ClaimResult{}, domain.ErrInvalidJobStatus
	case job.Attempts >= r.options.MaxAttempts:
		if _, err := r.failJob(ctx, tx, job, JobFailure{Code: "attempts_exhausted", Message: "processing attempt limit reached"}, now); err != nil {
			return ClaimResult{}, err
		}
		result.Disposition = ClaimExhausted
	case job.Status == domain.JobStatusRetrying && job.NextRetryAt.After(now):
		result.Disposition = ClaimWaitingRetry
	default:
		token, err := uuid.NewRandom()
		if err != nil {
			return ClaimResult{}, fmt.Errorf("generate attempt token: %w", err)
		}
		job, err = scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status='processing',attempts=attempts+1,attempt_token=$2,
   lease_until=clock_timestamp()+$3::bigint*interval '1 microsecond',next_retry_at=NULL,next_dispatch_at=NULL,updated_at=clock_timestamp()
   WHERE id=$1 AND status IN ('pending','retrying') AND attempts<$4 AND (next_retry_at IS NULL OR next_retry_at<=clock_timestamp())
   RETURNING `+jobColumns, job.ID, token.String(), r.options.LeaseDuration.Microseconds(), r.options.MaxAttempts))
		if err != nil {
			return ClaimResult{}, fmt.Errorf("claim job: %w", err)
		}
		key := resultObjectKey(job, locked.contentType)
		if _, err := tx.Exec(ctx, `INSERT INTO storage_objects(object_key,image_id,job_id,attempt_token,write_until)
   VALUES ($1,$2,$3,$4,$5)`, key, job.ImageID, job.ID, job.AttemptToken, job.LeaseUntil); err != nil {
			return ClaimResult{}, fmt.Errorf("register result intent: %w", err)
		}
		result = ClaimResult{Disposition: ClaimAcquired, Job: job, ResultKey: key, OriginalKey: locked.originalKey}
	}
	if err := commitJob(ctx, tx); err != nil {
		return ClaimResult{}, err
	}
	return result, nil
}

// JobResult contains the stored output metadata of the current attempt.
type JobResult = processor.JobResult

// JobFailure contains a safe code and message suitable for persistence.
type JobFailure = processor.JobFailure

// Complete accepts an output only from the current live attempt and its intent.
func (r *JobRepository) Complete(ctx context.Context, id, token string, result JobResult) (domain.Job, error) {
	if result.ObjectKey == "" || result.Width <= 0 || result.Height <= 0 || result.SizeBytes <= 0 {
		return domain.Job{}, ErrInvalidJobInput
	}
	return r.changeAttempt(ctx, id, token, attemptChange{status: domain.JobStatusCompleted, result: result})
}

// Retry schedules a supplied retry time, or fails the final allowed attempt.
// Backoff and jitter are the caller's responsibility.
func (r *JobRepository) Retry(ctx context.Context, id, token string, failure JobFailure, retryAt time.Time) (domain.Job, error) {
	if failure.Code == "" || failure.Message == "" || retryAt.IsZero() {
		return domain.Job{}, ErrInvalidJobInput
	}
	return r.changeAttempt(ctx, id, token, attemptChange{status: domain.JobStatusRetrying, failure: failure, retryAt: retryAt})
}

// Fail terminates the current live attempt and atomically enqueues a DLQ event.
func (r *JobRepository) Fail(ctx context.Context, id, token string, failure JobFailure) (domain.Job, error) {
	if failure.Code == "" || failure.Message == "" {
		return domain.Job{}, ErrInvalidJobInput
	}
	return r.changeAttempt(ctx, id, token, attemptChange{status: domain.JobStatusFailed, failure: failure})
}

type attemptChange struct {
	status  domain.JobStatus
	result  JobResult
	failure JobFailure
	retryAt time.Time
}

func (r *JobRepository) changeAttempt(ctx context.Context, id, token string, change attemptChange) (domain.Job, error) {
	parsed, err := uuid.Parse(token)
	if err != nil {
		return domain.Job{}, ErrAttemptFenced
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Job{}, fmt.Errorf("begin attempt change: %w", err)
	}
	defer tx.Rollback(ctx)
	locked, err := lockJob(ctx, tx, id)
	if err != nil {
		return domain.Job{}, err
	}
	job := locked.job
	if locked.deleted || job.Status != domain.JobStatusProcessing || job.AttemptToken != parsed.String() {
		return domain.Job{}, ErrAttemptFenced
	}
	key := resultObjectKey(job, locked.contentType)
	object, err := scanStorageObject(tx.QueryRow(ctx, "SELECT "+storageColumns+" FROM storage_objects WHERE object_key=$1 FOR UPDATE", key))
	if err != nil {
		return domain.Job{}, fmt.Errorf("lock result intent: %w", err)
	}
	now, err := jobClock(ctx, tx)
	if err != nil {
		return domain.Job{}, err
	}
	if object.ImageID != job.ImageID || object.JobID != job.ID || object.AttemptToken != job.AttemptToken || object.State != StorageStateWriting || !job.LeaseUntil.After(now) || !object.WriteUntil.After(now) {
		return domain.Job{}, ErrAttemptFenced
	}
	if change.status == domain.JobStatusCompleted && change.result.ObjectKey != key {
		return domain.Job{}, ErrAttemptFenced
	}
	if change.status == domain.JobStatusRetrying {
		if job.Attempts >= r.options.MaxAttempts {
			change.status = domain.JobStatusFailed
		} else if !change.retryAt.After(now) {
			return domain.Job{}, ErrInvalidJobInput
		}
	}
	var updated domain.Job
	if change.status == domain.JobStatusFailed {
		updated, err = r.failJob(ctx, tx, job, change.failure, now)
	} else {
		var resultKey, width, height, size, code, message, retryAt any
		if change.status == domain.JobStatusCompleted {
			resultKey = key
			width = change.result.Width
			height = change.result.Height
			size = change.result.SizeBytes
		} else {
			code = change.failure.Code
			message = change.failure.Message
			retryAt = change.retryAt
		}
		updated, err = scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status=$2,attempt_token=NULL,lease_until=NULL,
   result_key=$3,result_width=$4,result_height=$5,result_size_bytes=$6,last_error_code=$7,last_error=$8,
   next_retry_at=$9,next_dispatch_at=CASE WHEN $2='retrying' THEN $9::timestamptz+$10::bigint*interval '1 microsecond' END,
   completed_at=CASE WHEN $2='completed' THEN $11::timestamptz END,updated_at=$11
   WHERE id=$1 RETURNING `+jobColumns, job.ID, change.status, resultKey, width, height, size, code, message, retryAt, r.options.RedispatchInterval.Microseconds(), now))
		if err == nil && change.status == domain.JobStatusRetrying {
			err = enqueueJobRequest(ctx, tx, job.ID, r.options.JobsTopic, &change.retryAt)
		}
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("persist attempt outcome: %w", err)
	}
	state := StorageStateDiscarded
	if change.status == domain.JobStatusCompleted {
		state = StorageStateRetained
	}
	if _, err := tx.Exec(ctx, "UPDATE storage_objects SET state=$2,updated_at=clock_timestamp() WHERE object_key=$1", key, state); err != nil {
		return domain.Job{}, fmt.Errorf("finalize result intent: %w", err)
	}
	// Database writes may outlive the attempt lease.
	var live bool
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()<$1::timestamptz AND clock_timestamp()<$2::timestamptz", job.LeaseUntil, object.WriteUntil).Scan(&live); err != nil {
		return domain.Job{}, fmt.Errorf("recheck attempt lease: %w", err)
	}
	if !live {
		return domain.Job{}, ErrAttemptFenced
	}
	if err := commitJob(ctx, tx); err != nil {
		return domain.Job{}, err
	}
	return updated, nil
}

func (r *JobRepository) failJob(ctx context.Context, tx pgx.Tx, job domain.Job, failure JobFailure, now time.Time) (domain.Job, error) {
	updated, err := scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status='failed',attempt_token=NULL,lease_until=NULL,next_retry_at=NULL,next_dispatch_at=NULL,
  last_error_code=$2,last_error=$3,completed_at=$4,updated_at=$4 WHERE id=$1 RETURNING `+jobColumns, job.ID, failure.Code, failure.Message, now))
	if err != nil {
		return domain.Job{}, err
	}
	eventID, err := uuid.NewRandom()
	if err != nil {
		return domain.Job{}, fmt.Errorf("generate DLQ event ID: %w", err)
	}
	payload, err := json.Marshal(struct {
		EventID   string    `json:"event_id"`
		EventType string    `json:"event_type"`
		Version   int       `json:"version"`
		JobID     string    `json:"job_id"`
		ImageID   string    `json:"image_id"`
		Attempts  int       `json:"attempts"`
		ErrorCode string    `json:"error_code"`
		FailedAt  time.Time `json:"failed_at"`
	}{eventID.String(), "image.processing.failed", 1, job.ID, job.ImageID, job.Attempts, failure.Code, now.UTC()})
	if err != nil {
		return domain.Job{}, fmt.Errorf("serialize DLQ event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key,dedupe_key)
  VALUES ($1,$2,'image.processing.failed',$3,$4,$5,$6)`, eventID.String(), job.ID, payload, r.options.DLQTopic, job.ID, "job-failed:"+job.ID); err != nil {
		return domain.Job{}, fmt.Errorf("enqueue DLQ event: %w", err)
	}
	return updated, nil
}
