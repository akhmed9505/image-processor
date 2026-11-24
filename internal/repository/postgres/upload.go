package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Upload errors identify rejected input, expired intents, and uncertain commits.
var (
	ErrInvalidUpload         = errors.New("invalid upload")
	ErrWriteIntentExpired    = errors.New("original write intent expired")
	ErrUploadCommitUncertain = errors.New("upload commit outcome is uncertain")
)

// UploadRepository finalizes an original and its processing tasks atomically.
type UploadRepository struct {
	pool               *pgxpool.Pool
	jobsTopic          string
	redispatchInterval time.Duration
}

// NewUploadRepository configures the destination topic and redispatch delay.
// The caller owns pool; redispatchInterval must be at least one microsecond.
func NewUploadRepository(pool *pgxpool.Pool, jobsTopic string, redispatchInterval time.Duration) (*UploadRepository, error) {
	if jobsTopic == "" || redispatchInterval < time.Microsecond {
		return nil, ErrInvalidUpload
	}
	return &UploadRepository{pool: pool, jobsTopic: jobsTopic, redispatchInterval: redispatchInterval}, nil
}

// Create persists original metadata, pending jobs, and requested events together.
// It consumes image metadata and each job's ID, operation and parameters. Runtime
// job fields and creation timestamps are initialized here, not copied from input.
// On ErrUploadCommitUncertain, check image.ID and its intent before retry/cleanup.
func (r *UploadRepository) Create(ctx context.Context, image domain.Image) (domain.Image, error) {
	if len(image.Jobs) == 0 {
		return domain.Image{}, domain.ErrNoJobs
	}
	if len(image.Jobs) > 3 || image.ID == "" || image.OriginalKey == "" {
		return domain.Image{}, ErrInvalidUpload
	}
	if !image.DeletedAt.IsZero() {
		return domain.Image{}, domain.ErrImageDeleted
	}
	parameters := make([][]byte, len(image.Jobs))
	for i, job := range image.Jobs {
		if err := job.Params.Validate(job.Operation); err != nil {
			return domain.Image{}, fmt.Errorf("upload job parameters: %w", err)
		}
		var value any
		switch job.Operation {
		case domain.OperationResize:
			value = map[string]int{"width": job.Params.Resize.Width, "height": job.Params.Resize.Height}
		case domain.OperationThumbnail:
			value = map[string]int{"size": job.Params.Thumbnail.Size}
		case domain.OperationWatermark:
			value = struct{}{}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return domain.Image{}, fmt.Errorf("serialize upload parameters: %w", err)
		}
		parameters[i] = encoded
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Image{}, fmt.Errorf("begin upload: %w", err)
	}
	defer tx.Rollback(ctx)
	var ownsIntent, original bool
	var state StorageState
	err = tx.QueryRow(ctx, `SELECT image_id=$2::uuid,job_id IS NULL AND attempt_token IS NULL,state
 FROM storage_objects WHERE object_key=$1 FOR UPDATE`, image.OriginalKey, image.ID).Scan(&ownsIntent, &original, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Image{}, ErrStorageObjectNotFound
	}
	if err != nil {
		return domain.Image{}, fmt.Errorf("lock upload intent: %w", err)
	}
	if !ownsIntent || !original {
		return domain.Image{}, ErrInvalidWriteIntent
	}
	if state != StorageStateWriting {
		return domain.Image{}, ErrInvalidStorageTransition
	}
	// This statement starts only after FOR UPDATE has acquired the row lock.
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT write_until>clock_timestamp() FROM storage_objects WHERE object_key=$1`, image.OriginalKey).Scan(&valid); err != nil {
		return domain.Image{}, fmt.Errorf("check upload intent deadline: %w", err)
	}
	if !valid {
		return domain.Image{}, ErrWriteIntentExpired
	}
	err = tx.QueryRow(ctx, `INSERT INTO images(id,original_name,original_key,content_type,size_bytes,width,height)
 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id::text,created_at`, image.ID, image.OriginalName, image.OriginalKey, image.ContentType, image.SizeBytes, image.Width, image.Height).Scan(&image.ID, &image.CreatedAt)
	if err != nil {
		return domain.Image{}, fmt.Errorf("insert upload image: %w", err)
	}
	inputJobs := image.Jobs
	image.Jobs = make([]domain.Job, 0, len(inputJobs))
	for i, input := range inputJobs {
		job := domain.Job{ID: input.ID, ImageID: image.ID, Operation: input.Operation, Params: input.Params, Status: domain.JobStatusPending}
		err = tx.QueryRow(ctx, `INSERT INTO jobs(id,image_id,operation,params,status,next_dispatch_at)
   VALUES ($1,$2,$3,$4,'pending',clock_timestamp()+$5::bigint*interval '1 microsecond')
   RETURNING id::text,created_at,updated_at`, job.ID, job.ImageID, job.Operation, parameters[i], r.redispatchInterval.Microseconds()).Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
		if err != nil {
			return domain.Image{}, fmt.Errorf("insert upload job: %w", err)
		}
		if err := enqueueJobRequest(ctx, tx, job.ID, r.jobsTopic, nil); err != nil {
			return domain.Image{}, fmt.Errorf("insert upload outbox event: %w", err)
		}
		image.Jobs = append(image.Jobs, job)
	}
	// Recheck before retention as database work can itself outlive the intent.
	tag, err := tx.Exec(ctx, `UPDATE storage_objects SET state='retained',updated_at=clock_timestamp()
 WHERE object_key=$1 AND state='writing' AND write_until>clock_timestamp()`, image.OriginalKey)
	if err != nil {
		return domain.Image{}, fmt.Errorf("retain upload original: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.Image{}, ErrWriteIntentExpired
	}
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return domain.Image{}, fmt.Errorf("upload rolled back on commit: %w", err)
		}
		return domain.Image{}, fmt.Errorf("%w: %w", ErrUploadCommitUncertain, err)
	}
	return image, nil
}
