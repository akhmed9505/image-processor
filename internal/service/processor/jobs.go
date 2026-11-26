package processor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
)

// Attempt limits leave thirty seconds between the processing deadline and lease expiry.
const (
	AttemptTimeout     = 60 * time.Second
	AttemptLease       = 90 * time.Second
	MaxAttempts        = 5
	jobDatabaseTimeout = 5 * time.Second
)

// Attempt errors preserve infrastructure causes through wrapping.
var (
	ErrAttemptFenced      = errors.New("job attempt no longer owns a live lease")
	ErrJobCommitUncertain = errors.New("job commit outcome is uncertain")
	ErrOriginalNotFound   = errors.New("original object not found")
	ErrInvalidAdmission   = errors.New("admission does not contain an acquired attempt")
)

// ClaimDisposition describes a durable admission outcome.
type ClaimDisposition string

// Only ClaimAcquired grants permission to process the returned attempt.
const (
	ClaimAcquired     ClaimDisposition = "acquired"
	ClaimProcessing   ClaimDisposition = "processing"
	ClaimWaitingRetry ClaimDisposition = "waiting_retry"
	ClaimTerminal     ClaimDisposition = "terminal"
	ClaimImageDeleted ClaimDisposition = "image_deleted"
	ClaimExhausted    ClaimDisposition = "exhausted"
)

// ClaimResult contains attempt data only for ClaimAcquired.
type ClaimResult struct {
	Disposition ClaimDisposition
	Job         domain.Job
	ResultKey   string
	OriginalKey string
}

// CanAck reports a durable handoff. Claim errors must always return a zero result.
func (c ClaimResult) CanAck() bool {
	switch c.Disposition {
	case ClaimAcquired, ClaimProcessing, ClaimWaitingRetry, ClaimTerminal, ClaimImageDeleted, ClaimExhausted:
		return true
	}
	return false
}

// JobResult contains the stored output metadata of the current attempt.
type JobResult struct {
	ObjectKey     string
	Width, Height int
	SizeBytes     int64
}

// JobFailure contains a safe code and message suitable for persistence.
type JobFailure struct{ Code, Message string }

// JobRepository owns attempt transactions; configure AttemptLease and MaxAttempts.
type JobRepository interface {
	Claim(context.Context, string) (ClaimResult, error)
	Complete(context.Context, string, string, JobResult) (domain.Job, error)
	Retry(context.Context, string, string, JobFailure, time.Time) (domain.Job, error)
	Fail(context.Context, string, string, JobFailure) (domain.Job, error)
}

// JobStorage streams originals and results; missing originals wrap ErrOriginalNotFound.
type JobStorage interface {
	OpenOriginal(context.Context, string) (io.ReadCloser, error)
	Put(context.Context, string, io.Reader, int64, string) error
}

// JobService coordinates single attempts; dependencies remain caller-owned.
type JobService struct {
	repository JobRepository
	storage    JobStorage
}

// NewJobService uses the existing engine and spools encoded output to disk.
func NewJobService(repository JobRepository, storage JobStorage) *JobService {
	return &JobService{repository, storage}
}

// Admit must run after reserving a worker slot. An error must not acknowledge Kafka.
func (s *JobService) Admit(ctx context.Context, id string) (ClaimResult, error) {
	ctx, cancel := context.WithTimeout(ctx, jobDatabaseTimeout)
	defer cancel()
	result, err := s.repository.Claim(ctx, id)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("admit job %s: %w", id, err)
	}
	return result, nil
}

// ProcessAttempt runs only acquired admissions; cancellation leaves recovery to the lease.
// Finalization errors never trigger compensation, since COMMIT may have succeeded.
func (s *JobService) ProcessAttempt(ctx context.Context, admission ClaimResult) (domain.Job, error) {
	job := admission.Job
	if admission.Disposition != ClaimAcquired || job.Status != domain.JobStatusProcessing || job.ID == "" || job.AttemptToken == "" || admission.OriginalKey == "" || admission.ResultKey == "" || job.LeaseUntil.IsZero() || job.Attempts < 1 || job.Attempts > MaxAttempts {
		return domain.Job{}, ErrInvalidAdmission
	}
	// Time waiting after admission counts against the attempt's processing budget.
	deadline := time.Now().Add(AttemptTimeout)
	if cutoff := job.LeaseUntil.Add(AttemptTimeout - AttemptLease); cutoff.Before(deadline) {
		deadline = cutoff
	}
	attemptCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	result, err := s.process(attemptCtx, admission)
	if ctx.Err() != nil {
		return domain.Job{}, ctx.Err()
	}
	if attemptErr := attemptCtx.Err(); attemptErr != nil {
		err = attemptErr
	}
	if err == nil {
		completeCtx, stop := context.WithTimeout(attemptCtx, jobDatabaseTimeout)
		defer stop()
		completed, err := s.repository.Complete(completeCtx, job.ID, job.AttemptToken, result)
		if err != nil {
			return domain.Job{}, fmt.Errorf("complete job %s: %w", job.ID, err)
		}
		return completed, nil
	}
	failure, permanent := processingFailure(err)
	finalizeCtx, stop := context.WithTimeout(ctx, jobDatabaseTimeout)
	defer stop()
	var updated domain.Job
	var finalizeErr error
	if permanent || job.Attempts >= MaxAttempts {
		updated, finalizeErr = s.repository.Fail(finalizeCtx, job.ID, job.AttemptToken, failure)
	} else {
		updated, finalizeErr = s.repository.Retry(finalizeCtx, job.ID, job.AttemptToken, failure, time.Now().Add(RetryDelay(job.Attempts)))
	}
	if finalizeErr != nil {
		return domain.Job{}, fmt.Errorf("record job %s failure: %w", job.ID, errors.Join(err, finalizeErr))
	}
	return updated, fmt.Errorf("process job %s: %w", job.ID, err)
}

func (s *JobService) process(ctx context.Context, admission ClaimResult) (JobResult, error) {
	if err := ctx.Err(); err != nil {
		return JobResult{}, err
	}
	original, err := s.storage.OpenOriginal(ctx, admission.OriginalKey)
	if err != nil {
		return JobResult{}, fmt.Errorf("open original: %w", err)
	}
	originalClosed := false
	defer func() {
		if !originalClosed {
			original.Close()
		}
	}()
	file, err := os.CreateTemp("", "imageprocessor-result-*")
	if err != nil {
		return JobResult{}, fmt.Errorf("create result spool: %w", err)
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	result, err := Process(ctx, original, file, admission.Job.Operation, admission.Job.Params)
	closeErr := original.Close()
	originalClosed = true
	if err != nil {
		return JobResult{}, err
	}
	if closeErr != nil {
		return JobResult{}, fmt.Errorf("close original: %w", closeErr)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return JobResult{}, fmt.Errorf("rewind result: %w", err)
	}
	if err := s.storage.Put(ctx, admission.ResultKey, file, result.SizeBytes, result.ContentType); err != nil {
		return JobResult{}, fmt.Errorf("store result: %w", err)
	}
	return JobResult{ObjectKey: admission.ResultKey, Width: result.Width, Height: result.Height, SizeBytes: result.SizeBytes}, nil
}

func processingFailure(err error) (JobFailure, bool) {
	switch {
	case errors.Is(err, context.Canceled):
		return JobFailure{"attempt_cancelled", "processing was cancelled"}, false
	case errors.Is(err, context.DeadlineExceeded):
		return JobFailure{"attempt_timeout", "processing deadline exceeded"}, false
	case errors.Is(err, ErrOriginalNotFound):
		return JobFailure{"original_not_found", "original object is missing"}, true
	case errors.Is(err, ErrUnsupportedFormat), errors.Is(err, ErrInvalidImage):
		return JobFailure{"invalid_original", "original image cannot be decoded"}, true
	case errors.Is(err, ErrLimitExceeded):
		return JobFailure{"image_limit_exceeded", "image exceeds processing limits"}, true
	case errors.Is(err, domain.ErrInvalidOperation), errors.Is(err, domain.ErrInvalidOperationParams):
		return JobFailure{"invalid_operation", "operation parameters are invalid"}, true
	default:
		return JobFailure{"processing_unavailable", "processing dependency or local I/O failed"}, false
	}
}
