//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"image"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	objectstorage "github.com/akhmed9505/image-processor/internal/infra/storage"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func processingRepo(t *testing.T, p *pgxpool.Pool) *repository.JobRepository {
	t.Helper()
	r, e := repository.NewJobRepository(p, repository.JobRepositoryOptions{JobsTopic: "image.processing.jobs", DLQTopic: "image.processing.dlq", MaxAttempts: service.MaxAttempts, LeaseDuration: service.AttemptLease, RedispatchInterval: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func admittedJob(t *testing.T, p *pgxpool.Pool, s *objectstorage.Storage) (*service.JobService, service.ClaimResult) {
	t.Helper()
	uploaded, e := imageUpload(t, p, s).Upload(context.Background(), "input.png", bytes.NewReader(uploadBytes(t, "png")), uploadOperations()[:1])
	if e != nil {
		t.Fatal(e)
	}
	processor := service.NewJobService(processingRepo(t, p), s)
	admission, e := processor.Admit(context.Background(), uploaded.Jobs[0].ID)
	if e != nil || admission.Disposition != service.ClaimAcquired || !admission.CanAck() || admission.OriginalKey != uploaded.OriginalKey {
		t.Fatal("admission", admission, e)
	}
	return processor, admission
}

func TestJobServiceSuccess(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			p := migratedReadPool(t)
			s := uploadStorage(t)
			ctx := context.Background()
			uploaded, e := imageUpload(t, p, s).Upload(ctx, "input", bytes.NewReader(uploadBytes(t, format)), uploadOperations())
			if e != nil {
				t.Fatal(e)
			}
			processor := service.NewJobService(processingRepo(t, p), s)
			for i, job := range uploaded.Jobs {
				admission, e := processor.Admit(ctx, job.ID)
				if e != nil || !admission.CanAck() {
					t.Fatal(admission, e)
				}
				if admission.Job.Attempts != 1 || time.Until(admission.Job.LeaseUntil) < 89*time.Second {
					t.Fatal("lease or attempts", admission)
				}
				duplicate, e := processor.Admit(ctx, job.ID)
				if e != nil || duplicate.Disposition != service.ClaimProcessing || !duplicate.CanAck() {
					t.Fatal("duplicate claim", duplicate, e)
				}
				done, e := processor.ProcessAttempt(ctx, admission)
				if e != nil || done.Status != domain.JobStatusCompleted || done.ResultKey != admission.ResultKey {
					t.Fatal(done, e)
				}
				body, info, e := s.Get(ctx, done.ResultKey)
				if e != nil {
					t.Fatal(e)
				}
				decoded, detected, e := image.Decode(body)
				closeErr := body.Close()
				if e != nil || closeErr != nil {
					t.Fatal(e, closeErr)
				}
				expected := [][2]int{{60, 40}, {20, 20}, {120, 80}}[i]
				if detected != format || info.ContentType != "image/"+format || info.Size != done.ResultSizeBytes || decoded.Bounds().Dx() != expected[0] || decoded.Bounds().Dy() != expected[1] {
					t.Fatal("result metadata", info, done)
				}
				intent, e := repository.NewStorageRepository(p).GetByKey(ctx, admission.ResultKey)
				if e != nil || intent.State != repository.StorageStateRetained {
					t.Fatal(intent, e)
				}
				terminal, e := processor.Admit(ctx, job.ID)
				if e != nil || terminal.Disposition != service.ClaimTerminal || !terminal.CanAck() {
					t.Fatal(terminal, e)
				}
			}
		})
	}
}

type processingStorage struct {
	*objectstorage.Storage
	afterPut func(context.Context, string) error
}

func (s processingStorage) Put(ctx context.Context, key string, r io.Reader, n int64, ct string) error {
	if e := s.Storage.Put(ctx, key, r, n, ct); e != nil {
		return e
	}
	if s.afterPut != nil {
		return s.afterPut(ctx, key)
	}
	return nil
}

func TestJobServiceFailures(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "put acknowledgement lost", "cancel after put", "deleted after put", "complete rollback", "retry rollback", "fail rollback", "attempt timeout", "expired lease", "stale token", "final attempt"} {
		t.Run(kind, func(t *testing.T) {
			p := migratedReadPool(t)
			s := uploadStorage(t)
			processor, admission := admittedJob(t, p, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			expected := domain.JobStatusFailed
			var want error
			storedResult := false
			switch kind {
			case "missing":
				if e := s.Delete(ctx, admission.OriginalKey); e != nil {
					t.Fatal(e)
				}
				want = service.ErrOriginalNotFound
			case "corrupt":
				if e := s.Put(ctx, admission.OriginalKey, bytes.NewReader([]byte("broken")), 6, "image/png"); e != nil {
					t.Fatal(e)
				}
				want = service.ErrUnsupportedFormat
			case "put acknowledgement lost":
				want = io.ErrUnexpectedEOF
				expected = domain.JobStatusRetrying
				storedResult = true
				processor = service.NewJobService(processingRepo(t, p), processingStorage{s, func(context.Context, string) error { return want }})
			case "cancel after put":
				want = context.Canceled
				expected = domain.JobStatusProcessing
				storedResult = true
				processor = service.NewJobService(processingRepo(t, p), processingStorage{s, func(context.Context, string) error { cancel(); return ctx.Err() }})
			case "deleted after put":
				want = repository.ErrAttemptFenced
				expected = domain.JobStatusCancelled
				storedResult = true
				processor = service.NewJobService(processingRepo(t, p), processingStorage{s, func(c context.Context, _ string) error {
					return repository.NewDeleteRepository(p).Delete(c, admission.Job.ImageID)
				}})
			case "complete rollback", "retry rollback", "fail rollback":
				expected = domain.JobStatusProcessing
				if kind == "complete rollback" {
					storedResult = true
				} else if kind == "fail rollback" {
					if e := s.Delete(ctx, admission.OriginalKey); e != nil {
						t.Fatal(e)
					}
				}
				if _, e := p.Exec(ctx, `CREATE FUNCTION reject_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected finalization failure'; END $$;
 CREATE TRIGGER reject_attempt AFTER UPDATE ON jobs FOR EACH STATEMENT EXECUTE FUNCTION reject_attempt()`); e != nil {
					t.Fatal(e)
				}
				if kind == "retry rollback" {
					// A successful PUT followed by a lost acknowledgement takes the Retry path.
					storedResult = true
					processor = service.NewJobService(processingRepo(t, p), processingStorage{s, func(context.Context, string) error { return io.ErrUnexpectedEOF }})
				}
			case "attempt timeout":
				admission.Job.LeaseUntil = time.Now().Add(service.AttemptLease - service.AttemptTimeout - time.Second)
				expected = domain.JobStatusRetrying
				want = context.DeadlineExceeded
			case "expired lease":
				if _, e := p.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", admission.Job.ID); e != nil {
					t.Fatal(e)
				}
				expected = domain.JobStatusProcessing
				want = repository.ErrAttemptFenced
				storedResult = true
			case "stale token":
				admission.Job.AttemptToken = "00000000-0000-4000-8000-000000000099"
				expected = domain.JobStatusProcessing
				want = repository.ErrAttemptFenced
				storedResult = true
			case "final attempt":
				if _, e := p.Exec(ctx, "UPDATE jobs SET attempts=5 WHERE id=$1", admission.Job.ID); e != nil {
					t.Fatal(e)
				}
				admission.Job.Attempts = 5
				want = io.ErrUnexpectedEOF
				storedResult = true
				processor = service.NewJobService(processingRepo(t, p), processingStorage{s, func(context.Context, string) error { return want }})
			}
			_, e := processor.ProcessAttempt(ctx, admission)
			if e == nil || (want != nil && !errors.Is(e, want)) {
				t.Fatal("processing error", e)
			}
			if kind == "complete rollback" || kind == "retry rollback" || kind == "fail rollback" {
				var pgErr *pgconn.PgError
				if !errors.As(e, &pgErr) || pgErr.Code != "P0001" {
					t.Fatal("lost PostgreSQL error", e)
				}
			}
			var status domain.JobStatus
			var token *string
			var retryAt *time.Time
			if e := p.QueryRow(context.Background(), "SELECT status,attempt_token::text,next_retry_at FROM jobs WHERE id=$1", admission.Job.ID).Scan(&status, &token, &retryAt); e != nil {
				t.Fatal(e)
			}
			if status != expected {
				t.Fatal("unexpected state", status, expected)
			}
			if expected == domain.JobStatusRetrying && (retryAt == nil || time.Until(*retryAt) < 3*time.Second || time.Until(*retryAt) > 6*time.Second) {
				t.Fatal("retry schedule", retryAt)
			}
			if expected == domain.JobStatusProcessing && token == nil {
				t.Fatal("attempt lost without durable handoff")
			}
			if expected == domain.JobStatusFailed {
				var events int
				if e := p.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE job_id=$1 AND event_type='image.processing.failed'", admission.Job.ID).Scan(&events); e != nil || events != 1 {
					t.Fatal("missing DLQ event", e)
				}
			}
			intent, e := repository.NewStorageRepository(p).GetByKey(context.Background(), admission.ResultKey)
			if e != nil {
				t.Fatal(e)
			}
			expectedIntent := repository.StorageStateDiscarded
			if expected == domain.JobStatusProcessing {
				expectedIntent = repository.StorageStateWriting
			}
			if intent.State != expectedIntent || !intent.WriteUntil.Equal(admission.Job.LeaseUntil) && kind != "attempt timeout" {
				t.Fatal("result intent changed incorrectly", intent)
			}
			if storedResult {
				if _, e := s.Stat(context.Background(), admission.ResultKey); e != nil {
					t.Fatal("result was removed", e)
				}
			}
		})
	}
}

func TestJobServiceUncertainCommit(t *testing.T) {
	for _, stage := range []string{"claim", "complete", "retry", "fail"} {
		t.Run(stage, func(t *testing.T) {
			p := migratedReadPool(t)
			s := uploadStorage(t)
			_, admission := admittedJob(t, p, s)
			if stage == "claim" {
				// Start another pending job so only the tested admission uses the intercepted connection.
				uploaded, e := imageUpload(t, p, s).Upload(context.Background(), "input", bytes.NewReader(uploadBytes(t, "png")), uploadOperations()[:1])
				if e != nil {
					t.Fatal(e)
				}
				admission = service.ClaimResult{Job: uploaded.Jobs[0]}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			var storage service.JobStorage = s
			desired := domain.JobStatusCompleted
			switch stage {
			case "claim":
				desired = domain.JobStatusProcessing
			case "retry":
				desired = domain.JobStatusRetrying
				storage = processingStorage{s, func(context.Context, string) error { return io.ErrUnexpectedEOF }}
			case "fail":
				desired = domain.JobStatusFailed
				if e := s.Delete(ctx, admission.OriginalKey); e != nil {
					t.Fatal(e)
				}
			}
			processor := service.NewJobService(processingRepo(t, client), storage)
			type outcome struct {
				ack bool
				err error
			}
			done := make(chan outcome, 1)
			go func() {
				if stage == "claim" {
					a, e := processor.Admit(ctx, admission.Job.ID)
					done <- outcome{a.CanAck(), e}
				} else {
					_, e := processor.ProcessAttempt(ctx, admission)
					done <- outcome{false, e}
				}
			}()
			select {
			case <-blocked:
			case result := <-done:
				t.Fatal("COMMIT not reached", result.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			awaitOutboxState(t, ctx, func() (bool, error) {
				var status domain.JobStatus
				e := p.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", admission.Job.ID).Scan(&status)
				return status == desired, e
			})
			close(release)
			select {
			case result := <-done:
				if !errors.Is(result.err, service.ErrJobCommitUncertain) || !errors.Is(result.err, pgconn.ErrConnClosed) || result.ack {
					t.Fatal("unsafe commit handling", result)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var status domain.JobStatus
			if e := p.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", admission.Job.ID).Scan(&status); e != nil || status != desired {
				t.Fatal("committed state changed", status, e)
			}
			if stage == "complete" || stage == "retry" {
				if _, e := s.Stat(ctx, admission.ResultKey); e != nil {
					t.Fatal("uncertain result was deleted", e)
				}
				intent, e := repository.NewStorageRepository(p).GetByKey(ctx, admission.ResultKey)
				if e != nil {
					t.Fatal(e)
				}
				expected := repository.StorageStateRetained
				if stage == "retry" {
					expected = repository.StorageStateDiscarded
				}
				if intent.State != expected {
					t.Fatal("committed registry changed", intent)
				}
			}
		})
	}
}

func TestJobServiceTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	p := migratedReadPool(t)
	s := uploadStorage(t)
	processor, admission := admittedJob(t, p, s)
	if _, e := processor.ProcessAttempt(context.Background(), admission); e != nil {
		t.Fatal(e)
	}
	files, e := os.ReadDir(dir)
	if e != nil || len(files) != 0 {
		t.Fatal("temporary files leaked", files, e)
	}
}
