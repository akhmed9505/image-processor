//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedRecoveryServiceJob(t *testing.T, p *pgxpool.Pool) domain.Job {
	t.Helper()
	input := uploadInput()
	input.ID = uuid.NewString()
	input.OriginalKey = input.ID + "/original.png"
	input.Jobs = input.Jobs[:1]
	input.Jobs[0].ID = uuid.NewString()
	input.Jobs[0].ImageID = input.ID
	registerUpload(t, p, input)
	image, err := uploadRepo(t, p).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return image.Jobs[0]
}
func runRecoveryService(t *testing.T, r service.RecoveryRepository) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.NewRecoveryService(r, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(6 * time.Second):
				t.Error("recovery did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestRecoveryServiceConcurrent(t *testing.T) {
	p := migratedReadPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	jobs := processingRepo(t, p)
	attempts := make([]repository.ClaimResult, 5)
	for i := range attempts {
		job := seedRecoveryServiceJob(t, p)
		a, e := jobs.Claim(ctx, job.ID)
		if e != nil {
			t.Fatal(e)
		}
		attempts[i] = a
		if _, e := p.Exec(ctx, "UPDATE jobs SET attempts=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", job.ID, i+1); e != nil {
			t.Fatal(e)
		}
	}
	pending := seedRecoveryServiceJob(t, p)
	if _, e := p.Exec(ctx, "DELETE FROM outbox WHERE job_id=$1", pending.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second' WHERE id=$1", pending.ID); e != nil {
		t.Fatal(e)
	}
	r := repository.NewRecoveryRepository(jobs)
	stop1 := runRecoveryService(t, r)
	stop2 := runRecoveryService(t, r)
	awaitOutboxState(t, ctx, func() (bool, error) {
		var n int
		e := p.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='processing' OR (id=$1 AND next_dispatch_at<=clock_timestamp())", pending.ID).Scan(&n)
		return n == 0, e
	})
	stop1()
	stop2()
	bases := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second}
	for i, a := range attempts {
		image, e := repository.NewImageRepository(p).GetByID(ctx, a.Job.ImageID)
		if e != nil {
			t.Fatal(e)
		}
		job := image.Jobs[0]
		if job.Attempts != i+1 || job.AttemptToken != "" || !job.LeaseUntil.IsZero() || job.LastErrorCode != "lease_expired" {
			t.Fatal("bad recovery", job)
		}
		if i == 4 {
			if job.Status != domain.JobStatusFailed {
				t.Fatal(job)
			}
			assertJobDLQ(t, p, job)
		} else {
			delay := job.NextRetryAt.Sub(job.UpdatedAt)
			if job.Status != domain.JobStatusRetrying || delay < bases[i]*4/5 || delay > bases[i]*6/5 {
				t.Fatal("retry policy mismatch", job, delay)
			}
			var next time.Time
			if e := p.QueryRow(ctx, "SELECT next_dispatch_at FROM jobs WHERE id=$1", job.ID).Scan(&next); e != nil || !next.Equal(job.NextRetryAt.Add(service.RedispatchInterval)) {
				t.Fatal("redispatch schedule", next, e)
			}
		}
		object, e := repository.NewStorageRepository(p).GetByKey(ctx, a.ResultKey)
		if e != nil || object.State != repository.StorageStateDiscarded || !object.WriteUntil.Equal(a.Job.LeaseUntil) {
			t.Fatal("unsafe intent", object, e)
		}
		var n int
		if e := p.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE job_id=$1 AND event_type='image.processing.requested' AND published_at IS NULL", job.ID).Scan(&n); e != nil || n != 1 {
			t.Fatal("duplicate event", n, e)
		}
	}
	var events int
	if e := p.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE job_id=$1", pending.ID).Scan(&events); e != nil || events != 1 {
		t.Fatal("missing redispatch", events, e)
	}
}

func TestRecoveryServicePreservesPublication(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "backoff", true: "claim"}[claimed], func(t *testing.T) {
			p := migratedReadPool(t)
			job := seedRecoveryServiceJob(t, p)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second'"); e != nil {
				t.Fatal(e)
			}
			if claimed {
				if _, e := p.Exec(ctx, "UPDATE outbox SET claim_token=gen_random_uuid(),lease_until=clock_timestamp()+interval '30 seconds',publish_attempts=1"); e != nil {
					t.Fatal(e)
				}
			} else {
				if _, e := p.Exec(ctx, "UPDATE outbox SET available_at=clock_timestamp()+interval '2 minutes',last_error_code='publish_failed',publish_attempts=3"); e != nil {
					t.Fatal(e)
				}
			}
			before := outboxSnapshot(t, p)
			stop := runRecoveryService(t, repository.NewRecoveryRepository(processingRepo(t, p)))
			awaitOutboxState(t, ctx, func() (bool, error) {
				var advanced bool
				e := p.QueryRow(ctx, "SELECT next_dispatch_at>clock_timestamp() FROM jobs WHERE id=$1", job.ID).Scan(&advanced)
				return advanced, e
			})
			stop()
			if after := outboxSnapshot(t, p); after != before {
				t.Fatal("publication state changed")
			}
		})
	}
}

type recoveryServiceCommit struct {
	*repository.RecoveryRepository
	uncertain *repository.RecoveryRepository
	stage     string
	failed    chan error
}

func (r recoveryServiceCommit) Recover(ctx context.Context, n int, f func(int) time.Duration) ([]string, error) {
	if r.stage != "recover" {
		return r.RecoveryRepository.Recover(ctx, n, f)
	}
	ids, e := r.uncertain.Recover(ctx, n, f)
	r.failed <- e
	return ids, e
}
func (r recoveryServiceCommit) Redispatch(ctx context.Context, n int) ([]string, error) {
	if r.stage != "redispatch" {
		return r.RecoveryRepository.Redispatch(ctx, n)
	}
	ids, e := r.uncertain.Redispatch(ctx, n)
	r.failed <- e
	return ids, e
}
func TestRecoveryServiceUncertainCommit(t *testing.T) {
	for _, stage := range []string{"recover", "redispatch"} {
		t.Run(stage, func(t *testing.T) {
			p := migratedReadPool(t)
			job := seedRecoveryServiceJob(t, p)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if stage == "recover" {
				if _, e := processingRepo(t, p).Claim(ctx, job.ID); e != nil {
					t.Fatal(e)
				}
				expireAttempt(t, p)
			} else {
				if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second'; DELETE FROM outbox"); e != nil {
					t.Fatal(e)
				}
			}
			blocked, release := make(chan struct{}), make(chan struct{})
			cfg := p.Config()
			dial := cfg.ConnConfig.DialFunc
			cfg.ConnConfig.DialFunc = func(c context.Context, n, a string) (net.Conn, error) {
				conn, e := dial(c, n, a)
				if e != nil {
					return nil, e
				}
				return &uploadCommitResponseDrop{Conn: conn, ctx: ctx, blocked: blocked, release: release}, nil
			}
			client, e := pgxpool.NewWithConfig(ctx, cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { cancel(); client.Close() }()
			failed := make(chan error, 1)
			stop := runRecoveryService(t, recoveryServiceCommit{repository.NewRecoveryRepository(processingRepo(t, p)), repository.NewRecoveryRepository(processingRepo(t, client)), stage, failed})
			select {
			case <-blocked:
			case e := <-failed:
				t.Fatal("COMMIT not intercepted", e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			awaitOutboxState(t, ctx, func() (bool, error) {
				var committed bool
				predicate := "status='retrying'"
				if stage == "redispatch" {
					predicate = "next_dispatch_at>clock_timestamp()"
				}
				e := p.QueryRow(ctx, "SELECT "+predicate+" FROM jobs WHERE id=$1", job.ID).Scan(&committed)
				return committed, e
			})
			before := outboxSnapshot(t, p)
			close(release)
			select {
			case e := <-failed:
				if !errors.Is(e, service.ErrJobCommitUncertain) || !errors.Is(e, pgconn.ErrConnClosed) {
					t.Fatal("lost uncertainty", e)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stop()
			if outboxSnapshot(t, p) != before {
				t.Fatal("uncertain batch compensated")
			}
		})
	}
}
