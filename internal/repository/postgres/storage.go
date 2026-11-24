package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StorageState describes the lifecycle of a registered object.
type StorageState string

// Storage object states; discarded is terminal.
const (
	StorageStateWriting   StorageState = "writing"
	StorageStateRetained  StorageState = "retained"
	StorageStateDiscarded StorageState = "discarded"
)

// Storage registry errors can be checked with errors.Is.
var (
	ErrStorageObjectNotFound    = errors.New("storage object not found")
	ErrStorageObjectExists      = errors.New("storage object already registered")
	ErrInvalidStorageTransition = errors.New("invalid storage object transition")
	ErrInvalidWriteIntent       = errors.New("invalid original write intent")
)

// StorageObject is a registry record, not a domain image or a storage client DTO.
// JobID and AttemptToken are empty for original write intents.
type StorageObject struct {
	ObjectKey    string
	ImageID      string
	JobID        string
	AttemptToken string
	State        StorageState
	WriteUntil   time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// StorageRepository manages original write intents and reads registry records.
type StorageRepository struct{ pool *pgxpool.Pool }

// NewStorageRepository uses pool without taking ownership of its lifecycle.
func NewStorageRepository(pool *pgxpool.Pool) *StorageRepository {
	return &StorageRepository{pool: pool}
}

const storageColumns = `object_key,image_id::text,COALESCE(job_id::text,''),COALESCE(attempt_token::text,''),
 state,write_until,created_at,updated_at`

// RegisterOriginal reserves a new key before the image row exists.
// PostgreSQL computes the deadline. Existing keys are never overwritten or reused.
// TTL must be at least one microsecond, matching PostgreSQL timestamp precision.
func (r *StorageRepository) RegisterOriginal(ctx context.Context, key, imageID string, ttl time.Duration) (StorageObject, error) {
	if key == "" || imageID == "" || ttl < time.Microsecond {
		return StorageObject{}, ErrInvalidWriteIntent
	}
	object, err := scanStorageObject(r.pool.QueryRow(ctx, `INSERT INTO storage_objects(object_key,image_id,write_until)
 VALUES ($1,$2,clock_timestamp()+$3::bigint*interval '1 microsecond')
 ON CONFLICT (object_key) DO NOTHING RETURNING `+storageColumns, key, imageID, ttl.Microseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return StorageObject{}, ErrStorageObjectExists
	}
	if err != nil {
		return StorageObject{}, fmt.Errorf("register original write intent: %w", err)
	}
	return object, nil
}

// GetByKey returns a registry record, including discarded and expired records.
func (r *StorageRepository) GetByKey(ctx context.Context, key string) (StorageObject, error) {
	object, err := scanStorageObject(r.pool.QueryRow(ctx, `SELECT `+storageColumns+` FROM storage_objects WHERE object_key=$1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return StorageObject{}, ErrStorageObjectNotFound
	}
	if err != nil {
		return StorageObject{}, fmt.Errorf("get storage object: %w", err)
	}
	return object, nil
}

// DiscardOriginal abandons an original write intent that is still writing.
// Retained, discarded, and job-owned records are never modified.
func (r *StorageRepository) DiscardOriginal(ctx context.Context, key string) (StorageObject, error) {
	object, err := scanStorageObject(r.pool.QueryRow(ctx, `UPDATE storage_objects
 SET state='discarded',updated_at=clock_timestamp()
 WHERE object_key=$1 AND state='writing' AND job_id IS NULL AND attempt_token IS NULL
 RETURNING `+storageColumns, key))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := r.GetByKey(ctx, key); err != nil {
			return StorageObject{}, err
		}
		return StorageObject{}, ErrInvalidStorageTransition
	}
	if err != nil {
		return StorageObject{}, fmt.Errorf("discard original write intent: %w", err)
	}
	return object, nil
}

func scanStorageObject(row pgx.Row) (StorageObject, error) {
	var object StorageObject
	err := row.Scan(&object.ObjectKey, &object.ImageID, &object.JobID, &object.AttemptToken, &object.State, &object.WriteUntil, &object.CreatedAt, &object.UpdatedAt)
	return object, err
}
