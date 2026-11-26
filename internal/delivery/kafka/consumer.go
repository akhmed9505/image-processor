// Package kafka delivers durable job notifications to a bounded worker pool.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	workerCount       = 2
	offsetTimeout     = 5 * time.Second
	shutdownTimeout   = 30 * time.Second
	workerStopTimeout = 5 * time.Second
	admissionBackoff  = time.Second
)

// Client must disable auto-commit and use BlockRebalanceOnPoll.
// The caller owns the client and closes it after Run returns.
type Client interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	AllowRebalance()
}

// Jobs performs durable admission and owns all attempt finalization.
// Both methods must honor context cancellation.
type Jobs interface {
	Admit(context.Context, string) (processor.ClaimResult, error)
	ProcessAttempt(context.Context, processor.ClaimResult) (domain.Job, error)
}

// ShutdownError means some attempts ignored forced cancellation for five seconds.
// Dependencies must remain open until Done closes, even though Run has returned.
type ShutdownError struct{ Done <-chan struct{} }

func (*ShutdownError) Error() string { return "workers did not stop after forced cancellation" }

// Consumer reserves capacity before polling and admitting a single notification.
type Consumer struct {
	client Client
	jobs   Jobs
	logger *slog.Logger
}

// New uses two workers without an application queue; dependencies remain caller-owned.
func New(client Client, jobs Jobs, logger *slog.Logger) *Consumer {
	return &Consumer{client, jobs, logger.With("component", "job_consumer")}
}

// Run drains for thirty seconds, then cancels attempts and waits up to five seconds.
// A nil result guarantees worker cleanup is complete; ShutdownError exposes pending completion.
func (c *Consumer) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	slots := make(chan struct{}, workerCount)
	allDone := make(chan struct{})
	var pending atomic.Int32
	pending.Store(1) // Keep completion open until admissions have stopped.
	workerDone := func() {
		if pending.Add(-1) == 0 {
			close(allDone)
		}
	}
	for ctx.Err() == nil {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			goto shutdown
		}
		if ctx.Err() != nil {
			<-slots
			break
		}
		fetches := c.client.PollRecords(ctx, 1)
		records := fetches.Records()
		if len(records) == 0 {
			<-slots
			c.client.AllowRebalance()
			if ctx.Err() != nil {
				break
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				c.logger.ErrorContext(ctx, "Kafka poll failed", "error", errs[0].Err)
				if !wait(ctx, admissionBackoff) {
					break
				}
			}
			continue
		}
		record := records[0]
		ack := false
		jobID, err := decodeRequested(record)
		if err != nil {
			c.logger.ErrorContext(ctx, "Invalid job notification", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
			ack = true
		} else {
			admission, admitErr := c.jobs.Admit(ctx, jobID)
			if admitErr == nil && admission.CanAck() {
				ack = true
				if admission.Disposition == processor.ClaimAcquired {
					pending.Add(1)
					go func() {
						defer func() {
							<-slots
							workerDone()
						}()
						if _, err := c.jobs.ProcessAttempt(workCtx, admission); err != nil {
							c.logger.WarnContext(workCtx, "Job attempt ended with error", "job_id", jobID, "error", err)
						}
					}()
				} else {
					<-slots
				}
			} else {
				<-slots
				if admitErr == nil {
					admitErr = errors.New("admission has no durable disposition")
				}
				c.logger.WarnContext(ctx, "Job admission deferred", "job_id", jobID, "error", admitErr)
			}
		}
		if err != nil {
			<-slots
		}
		if ack {
			commitCtx, cancel := context.WithTimeout(ctx, offsetTimeout)
			err = c.client.CommitRecords(commitCtx, record)
			cancel()
			if err != nil {
				c.logger.WarnContext(ctx, "Kafka offset commit deferred", "partition", record.Partition, "offset", record.Offset, "error", err)
				ack = false
			}
		}
		// Rewind before releasing the rebalance gate; never consume past an unacknowledged record.
		if !ack {
			c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}}})
		}
		c.client.AllowRebalance()
		if !ack && !wait(ctx, admissionBackoff) {
			break
		}
	}
shutdown:
	c.client.AllowRebalance()
	workerDone() // Release the admission sentinel.
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()
	select {
	case <-allDone:
		return nil
	case <-timer.C:
	}
	cancelWork()
	c.logger.Warn("Worker shutdown deadline exceeded", "active_attempts", pending.Load())
	stopTimer := time.NewTimer(workerStopTimeout)
	defer stopTimer.Stop()
	select {
	case <-allDone:
		return nil
	case <-stopTimer.C:
		return &ShutdownError{Done: allDone}
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func decodeRequested(record *kgo.Record) (string, error) {
	var event struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		Version   int    `json:"version"`
		JobID     string `json:"job_id"`
	}
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return "", errors.New("invalid event JSON")
	}
	if event.EventType != "image.processing.requested" || event.Version != 1 {
		return "", errors.New("unsupported event type or version")
	}
	for _, id := range []string{event.EventID, event.JobID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return "", errors.New("invalid event or job ID")
		}
	}
	if string(record.Key) != event.JobID {
		return "", errors.New("message key does not match job ID")
	}
	return event.JobID, nil
}
