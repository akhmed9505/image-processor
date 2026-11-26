package image

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Cleanup timings bound storage calls within the tombstone claim lease.
const (
	CleanupGrace           = 10 * time.Minute
	CleanupLease           = 90 * time.Second
	cleanupInterval        = time.Minute
	cleanupPassTimeout     = 60 * time.Second
	cleanupDatabaseTimeout = 5 * time.Second
	cleanupStorageTimeout  = 30 * time.Second
	cleanupBatchSize       = 100
)

// Cleanup errors preserve their database causes.
var (
	ErrCleanupFenced          = errors.New("cleanup claim no longer owns a live lease")
	ErrCleanupCommitUncertain = errors.New("cleanup commit outcome is uncertain")
)

// StorageState describes a registered object's lifecycle.
type StorageState string

// Discarded keys are never reused or retained again.
const (
	StorageStateWriting   StorageState = "writing"
	StorageStateRetained  StorageState = "retained"
	StorageStateDiscarded StorageState = "discarded"
)

// StorageObject identifies an immutable key and its write intent.
type StorageObject struct {
	ObjectKey, ImageID, JobID, AttemptToken string
	State                                   StorageState
	WriteUntil, CreatedAt, UpdatedAt        time.Time
}

// CleanupClaim owns one deleted image's cleanup scheduling.
type CleanupClaim struct {
	ImageID, Token string
	LeaseUntil     time.Time
}

// CleanupPage separates safe deletion candidates from scan progress.
type CleanupPage struct {
	Objects    []StorageObject
	NextCursor string
}

// CleanupRepository authorizes deletion transactionally; errors authorize no objects.
// Claim must use CleanupLease. Empty Objects does not imply the scan has ended.
type CleanupRepository interface {
	Claim(context.Context, int) ([]CleanupClaim, error)
	FindDeletable(context.Context, int, string, time.Duration) (CleanupPage, error)
	FindDeletableForImage(context.Context, int, string, time.Duration, CleanupClaim) (CleanupPage, error)
	Complete(context.Context, string, string, time.Time) error
	Reschedule(context.Context, string, string, time.Time) error
}

// CleanupRegistry reads records that must remain discarded after authorization.
type CleanupRegistry interface {
	GetByKey(context.Context, string) (StorageObject, error)
}

// CleanupStorage deletes absent keys idempotently and honors context cancellation.
type CleanupStorage interface {
	Delete(context.Context, string) error
}

// CleanupService retains registry keys and repeats scans to catch late PUTs.
type CleanupService struct {
	repository CleanupRepository
	registry   CleanupRegistry
	storage    CleanupStorage
	logger     *slog.Logger
}

// NewCleanupService leaves dependency ownership with the caller.
func NewCleanupService(repository CleanupRepository, registry CleanupRegistry, storage CleanupStorage, logger *slog.Logger) *CleanupService {
	return &CleanupService{repository, registry, storage, logger.With("component", "storage_cleanup")}
}

// Run owns its scan cursor; each invocation must have its own service loop.
func (s *CleanupService) Run(ctx context.Context) error {
	scan := cleanupScan{}
	for {
		if ctx.Err() != nil {
			return nil
		}
		s.cleanupPass(ctx, &scan)
		if ctx.Err() != nil {
			return nil
		}
		timer := time.NewTimer(cleanupInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// cleanupScan retains partial failure information until a complete scan ends.
type cleanupScan struct {
	cursor string
	failed bool
}

func (s *CleanupService) cleanupPass(ctx context.Context, scan *cleanupScan) {
	s.cleanupTombstone(ctx)
	if ctx.Err() != nil {
		return
	}
	passCtx, cancel := context.WithTimeout(ctx, cleanupPassTimeout)
	defer cancel()
	if err := s.sweep(passCtx, scan, nil); err != nil {
		s.logError(ctx, "Global object cleanup deferred", err)
	}
}

func (s *CleanupService) cleanupTombstone(ctx context.Context) {
	dbCtx, cancel := context.WithTimeout(ctx, cleanupDatabaseTimeout)
	claims, err := s.repository.Claim(dbCtx, 1)
	cancel()
	if err != nil {
		s.logError(ctx, "Cleanup claim deferred", err)
		return
	}
	if len(claims) == 0 {
		return
	}
	claim := claims[0]
	passCtx, stop := context.WithTimeout(ctx, cleanupPassTimeout)
	defer stop()
	passCtx, cancel = context.WithDeadline(passCtx, claim.LeaseUntil.Add(-cleanupDatabaseTimeout))
	defer cancel()
	scan := cleanupScan{}
	err = s.sweep(passCtx, &scan, &claim)
	if err == nil {
		err = passCtx.Err()
	}
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		s.logError(ctx, "Image cleanup deferred", err)
	}
	dbCtx, cancel = context.WithTimeout(ctx, cleanupDatabaseTimeout)
	defer cancel()
	next := time.Now().Add(cleanupInterval)
	if err != nil {
		err = s.repository.Reschedule(dbCtx, claim.ImageID, claim.Token, next)
	} else {
		err = s.repository.Complete(dbCtx, claim.ImageID, claim.Token, next)
	}
	if err != nil {
		s.logError(ctx, "Cleanup finalization deferred", err)
	}
}

func (s *CleanupService) sweep(ctx context.Context, scan *cleanupScan, claim *CleanupClaim) error {
	for {
		if err := ctx.Err(); err != nil {
			scan.failed = true
			return err
		}
		dbCtx, cancel := context.WithTimeout(ctx, cleanupDatabaseTimeout)
		var page CleanupPage
		var err error
		if claim == nil {
			page, err = s.repository.FindDeletable(dbCtx, cleanupBatchSize, scan.cursor, CleanupGrace)
		} else {
			page, err = s.repository.FindDeletableForImage(dbCtx, cleanupBatchSize, scan.cursor, CleanupGrace, *claim)
		}
		cancel()
		if err != nil {
			scan.failed = true
			return err
		}
		for _, object := range page.Objects {
			if err := ctx.Err(); err != nil {
				scan.failed = true
				return err
			}
			if err := s.deleteObject(ctx, object.ObjectKey); err != nil {
				s.logError(ctx, "Object deletion deferred", fmt.Errorf("key %s: %w", object.ObjectKey, err))
				scan.failed = true
			}
			scan.cursor = object.ObjectKey
		}
		if err := ctx.Err(); err != nil {
			scan.failed = true
			return err
		}
		scan.cursor = page.NextCursor
		if scan.cursor == "" {
			failed := scan.failed
			scan.failed = false
			if failed {
				return errors.New("object cleanup scan encountered failures")
			}
			return nil
		}
	}
}

func (s *CleanupService) deleteObject(ctx context.Context, key string) error {
	dbCtx, cancel := context.WithTimeout(ctx, cleanupDatabaseTimeout)
	object, err := s.registry.GetByKey(dbCtx, key)
	cancel()
	if err != nil {
		return err
	}
	if object.State != StorageStateDiscarded {
		return errors.New("authorized object is no longer discarded")
	}
	storageCtx, cancel := context.WithTimeout(ctx, cleanupStorageTimeout)
	defer cancel()
	if err := s.storage.Delete(storageCtx, key); err != nil {
		return err
	}
	s.logger.DebugContext(ctx, "Storage object deleted", "object_key", key)
	return nil
}

func (s *CleanupService) logError(ctx context.Context, message string, err error) {
	if ctx.Err() != nil {
		return
	}
	level := slog.LevelError
	if errors.Is(err, ErrCleanupFenced) {
		level = slog.LevelDebug
	} else if errors.Is(err, ErrCleanupCommitUncertain) {
		level = slog.LevelWarn
	}
	s.logger.Log(ctx, level, message, "error", err)
}
