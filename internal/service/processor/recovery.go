package processor

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Recovery timings complement the JobRepository's MaxAttempts and AttemptLease.
const (
	RecoveryInterval   = 5 * time.Second
	RedispatchInterval = 60 * time.Second
	recoveryBatchSize  = 100
)

// RecoveryRepository owns transactions and concurrent batch exclusion.
// Configure its JobRepository with MaxAttempts and RedispatchInterval.
type RecoveryRepository interface {
	Recover(context.Context, int, func(int) time.Duration) ([]string, error)
	Redispatch(context.Context, int) ([]string, error)
}

// RecoveryService restores expired attempts and missing durable notifications.
type RecoveryService struct {
	repository RecoveryRepository
	logger     *slog.Logger
}

// NewRecoveryService leaves dependency ownership with the caller.
func NewRecoveryService(repository RecoveryRepository, logger *slog.Logger) *RecoveryService {
	return &RecoveryService{repository, logger.With("component", "job_recovery")}
}

// Run processes bounded batches immediately, then waits between passes. Cancellation returns nil.
func (s *RecoveryService) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		s.recoverPass(ctx)
		if ctx.Err() != nil {
			return nil
		}
		timer := time.NewTimer(RecoveryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (s *RecoveryService) recoverPass(ctx context.Context) {
	recoveryCtx, cancel := context.WithTimeout(ctx, jobDatabaseTimeout)
	ids, err := s.repository.Recover(recoveryCtx, recoveryBatchSize, RetryDelay)
	cancel()
	if ctx.Err() != nil {
		return
	}
	s.logBatch(ctx, "recover", ids, err)
	redispatchCtx, cancel := context.WithTimeout(ctx, jobDatabaseTimeout)
	defer cancel()
	ids, err = s.repository.Redispatch(redispatchCtx, recoveryBatchSize)
	if ctx.Err() == nil {
		s.logBatch(ctx, "redispatch", ids, err)
	}
}

func (s *RecoveryService) logBatch(ctx context.Context, operation string, ids []string, err error) {
	if err != nil {
		level := slog.LevelError
		if errors.Is(err, ErrJobCommitUncertain) {
			level = slog.LevelWarn
		}
		s.logger.Log(ctx, level, "Job recovery batch deferred", "operation", operation, "error", err)
		return
	}
	if len(ids) > 0 {
		s.logger.InfoContext(ctx, "Job recovery batch committed", "operation", operation, "jobs", len(ids))
	}
}
