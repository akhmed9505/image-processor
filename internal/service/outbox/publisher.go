// Package outbox publishes durable notifications to Kafka.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Publication timings leave room for database finalization within the lease.
const (
	PublishTimeout  = 10 * time.Second
	ClaimLease      = 30 * time.Second
	databaseTimeout = 5 * time.Second
	pollInterval    = time.Second
)

// Repository errors retain their original database causes.
var (
	ErrClaimFenced     = errors.New("outbox claim no longer owns a live lease")
	ErrCommitUncertain = errors.New("outbox commit outcome is uncertain")
)

// Event is a leased notification with its original wire payload.
type Event struct {
	ID, EventType, Topic, MessageKey string
	Payload                          json.RawMessage
	ClaimToken                       string
	LeaseUntil                       time.Time
	PublishAttempts                  int
}

// Repository owns claim and finalization transactions; Claim must use ClaimLease.
type Repository interface {
	Claim(context.Context, int) ([]Event, error)
	MarkPublished(context.Context, string, string) error
	Reschedule(context.Context, string, string, string, time.Time) error
}

// Producer returns nil only after broker acknowledgement and must honor context cancellation.
type Producer interface {
	Publish(context.Context, string, []byte, []byte) error
}

// Publisher owns no dependency resources; Run is stopped through its context.
type Publisher struct {
	repository Repository
	producer   Producer
	logger     *slog.Logger
}

// New uses a single-event claim to avoid sequential batch lease exhaustion.
func New(repository Repository, producer Producer, logger *slog.Logger) *Publisher {
	return &Publisher{repository, producer, logger.With("component", "outbox_publisher")}
}

// Run drains available events and waits on empty polls or errors. Cancellation returns nil.
func (p *Publisher) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		worked, err := p.publishOne(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !worked {
			logRepositoryError(ctx, p.logger, "Outbox claim deferred", err)
		}
		if !worked || err != nil {
			timer := time.NewTimer(pollInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil
			}
		}
	}
}

func (p *Publisher) publishOne(ctx context.Context) (bool, error) {
	claimCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	events, err := p.repository.Claim(claimCtx, 1)
	cancel()
	if err != nil {
		return false, fmt.Errorf("claim outbox: %w", err)
	}
	if len(events) == 0 {
		return false, nil
	}
	event := events[0]
	logger := p.logger.With("event_id", event.ID, "topic", event.Topic, "publish_attempts", event.PublishAttempts)
	if err := ctx.Err(); err != nil {
		return true, err
	}
	publishCtx, cancel := context.WithTimeout(ctx, PublishTimeout)
	err = p.producer.Publish(publishCtx, event.Topic, []byte(event.MessageKey), event.Payload)
	timedOut := errors.Is(publishCtx.Err(), context.DeadlineExceeded)
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	finalizeCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	if err != nil {
		code := "publish_failed"
		if timedOut || errors.Is(err, context.DeadlineExceeded) {
			code = "publish_timeout"
		}
		logger.WarnContext(ctx, "Kafka publication failed", "error_code", code, "error", err)
		if retryErr := p.repository.Reschedule(finalizeCtx, event.ID, event.ClaimToken, code, time.Now().Add(backoff(event.PublishAttempts))); retryErr != nil {
			logRepositoryError(ctx, logger, "Outbox reschedule deferred", retryErr)
			return true, fmt.Errorf("reschedule event %s: %w", event.ID, errors.Join(err, retryErr))
		}
		return true, err
	}
	// An unconfirmed SQL commit is left for lease expiry, never forcibly rescheduled.
	if err := p.repository.MarkPublished(finalizeCtx, event.ID, event.ClaimToken); err != nil {
		logRepositoryError(ctx, logger, "Outbox acknowledgement deferred", err)
		return true, fmt.Errorf("mark event %s published: %w", event.ID, err)
	}
	logger.DebugContext(ctx, "Outbox event published")
	return true, nil
}

func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	return min(30*time.Second, time.Second<<min(attempts-1, 5))
}

func logRepositoryError(ctx context.Context, logger *slog.Logger, message string, err error) {
	level := slog.LevelError
	if errors.Is(err, ErrClaimFenced) {
		level = slog.LevelDebug
	} else if errors.Is(err, ErrCommitUncertain) {
		level = slog.LevelWarn
	}
	logger.Log(ctx, level, message, "error", err)
}
