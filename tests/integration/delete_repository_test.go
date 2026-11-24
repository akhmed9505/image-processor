//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func deleteSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var state string
	err := pool.QueryRow(context.Background(), `SELECT jsonb_build_array(
 (SELECT jsonb_agg(to_jsonb(i) ORDER BY id) FROM images i),
 (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM jobs j),
 (SELECT jsonb_agg(to_jsonb(s) ORDER BY object_key) FROM storage_objects s),
 (SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM outbox o))::text`).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestDeleteJobStates(t *testing.T) {
	for _, status := range []domain.JobStatus{domain.JobStatusPending, domain.JobStatusProcessing, domain.JobStatusRetrying, domain.JobStatusCompleted, domain.JobStatusFailed, domain.JobStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			pool := migratedReadPool(t)
			job := seedJob(t, pool)
			ctx := context.Background()
			var attempt repository.ClaimResult
			if status != domain.JobStatusPending && status != domain.JobStatusCancelled {
				attempt = claimJob(t, pool, job.ID)
				var err error
				switch status {
				case domain.JobStatusCompleted:
					_, err = jobRepo(t, pool).Complete(ctx, job.ID, attempt.Job.AttemptToken, resultFor(attempt))
				case domain.JobStatusRetrying:
					_, err = jobRepo(t, pool).Retry(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure, retryTime(t, pool))
				case domain.JobStatusFailed:
					_, err = jobRepo(t, pool).Fail(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if status == domain.JobStatusCancelled {
				if _, err := pool.Exec(ctx, "UPDATE jobs SET status='cancelled',completed_at=clock_timestamp()"); err != nil {
					t.Fatal(err)
				}
			}
			before, err := repository.NewImageRepository(pool).GetByID(ctx, job.ImageID)
			if err != nil {
				t.Fatal(err)
			}
			// Keep published history and an unrelated event; only pending requests disappear.
			if _, err := pool.Exec(ctx, `INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key,published_at)
 VALUES ($1,$2::uuid,'image.processing.requested','{}','jobs',$2::uuid::text,clock_timestamp())`, uuid.NewString(), job.ID); err != nil {
				t.Fatal(err)
			}
			seedOutbox(t, pool, 1)
			// A previous, already discarded attempt can still have a PUT in flight.
			deadline := retryTime(t, pool)
			if _, err := pool.Exec(ctx, `INSERT INTO storage_objects(object_key,image_id,job_id,attempt_token,state,write_until)
 VALUES ('old-result',$1,$2,$3,'discarded',$4)`, job.ImageID, job.ID, uuid.NewString(), deadline); err != nil {
				t.Fatal(err)
			}
			objects, err := pgx.CollectRows(mustStorageRows(t, pool), pgx.RowToMap)
			if err != nil {
				t.Fatal(err)
			}
			var event repository.OutboxEvent
			if status != domain.JobStatusRetrying { // Retrying requests are deliberately delayed.
				events, err := outboxRepo(t, pool).Claim(ctx, 10)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range events {
					if e.EventType == "image.processing.requested" {
						event = e
					}
				}
			}
			var beforeJob, kept string
			if err := pool.QueryRow(ctx, "SELECT to_jsonb(j)::text FROM jobs j WHERE id=$1", job.ID).Scan(&beforeJob); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(o) ORDER BY id)::text FROM outbox o WHERE published_at IS NOT NULL OR event_type<>'image.processing.requested'`).Scan(&kept); err != nil {
				t.Fatal(err)
			}
			if err := repository.NewDeleteRepository(pool).Delete(ctx, job.ImageID); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.NewImageRepository(pool).GetByID(ctx, job.ImageID); !errors.Is(err, domain.ErrImageNotFound) {
				t.Fatalf("deleted image visible: %v", err)
			}
			page, err := repository.NewImageRepository(pool).List(ctx, 20, nil)
			if err != nil || len(page.Images) != 0 {
				t.Fatalf("deleted image listed: %+v %v", page, err)
			}
			var tombstone, cleanup time.Time
			if err := pool.QueryRow(ctx, "SELECT deleted_at,cleanup_after FROM images WHERE id=$1", job.ImageID).Scan(&tombstone, &cleanup); err != nil || tombstone.IsZero() || cleanup.Before(deadline) {
				t.Fatalf("unsafe cleanup schedule: %v %v %v", tombstone, cleanup, err)
			}
			var stored string
			if err := pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", job.ID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			expected := domain.JobStatusCancelled
			if status.IsTerminal() {
				expected = status
			}
			if stored != string(expected) {
				t.Fatalf("status=%s want %s", stored, expected)
			}
			var preserved bool
			if err := pool.QueryRow(ctx, `SELECT original_name=$2 AND original_key=$3 AND size_bytes=$4 AND width=$5 AND height=$6 AND created_at=$7 FROM images WHERE id=$1`, job.ImageID, before.OriginalName, before.OriginalKey, before.SizeBytes, before.Width, before.Height, before.CreatedAt).Scan(&preserved); err != nil || !preserved {
				t.Fatal("image metadata changed", err)
			}
			if !status.IsTerminal() {
				if err := pool.QueryRow(ctx, `SELECT attempt_token IS NULL AND lease_until IS NULL AND next_retry_at IS NULL AND next_dispatch_at IS NULL AND completed_at=$2 AND updated_at=$2 FROM jobs WHERE id=$1`, job.ID, tombstone).Scan(&preserved); err != nil || !preserved {
					t.Fatal("cancelled job retains an active claim or schedule", err)
				}
			} else {
				var afterJob string
				if err := pool.QueryRow(ctx, "SELECT to_jsonb(j)::text FROM jobs j WHERE id=$1", job.ID).Scan(&afterJob); err != nil {
					t.Fatal(err)
				}
				if beforeJob != afterJob {
					t.Fatal("terminal job metadata changed")
				}

			}
			afterObjects, err := pgx.CollectRows(mustStorageRows(t, pool), pgx.RowToMap)
			if err != nil {
				t.Fatal(err)
			}
			if len(objects) != len(afterObjects) {
				t.Fatal("registry rows removed")
			}
			for i, old := range objects {
				now := afterObjects[i]
				if now["state"] != "discarded" {
					t.Fatal("object still retained or writing")
				}
				delete(old, "state")
				delete(old, "updated_at")
				delete(now, "state")
				delete(now, "updated_at")
				if !reflect.DeepEqual(old, now) {
					t.Fatal("object identity or write deadline changed")
				}
			}
			var afterKept string
			if err := pool.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(o) ORDER BY id)::text FROM outbox o WHERE published_at IS NOT NULL OR event_type<>'image.processing.requested'`).Scan(&afterKept); err != nil {
				t.Fatal(err)
			}
			if kept != afterKept {
				t.Fatal("published history, DLQ, or unrelated event changed")
			}
			if event.ID != "" {
				if err := outboxRepo(t, pool).MarkPublished(ctx, event.ID, event.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
					t.Fatalf("old publisher accepted: %v", err)
				}
				if err := outboxRepo(t, pool).Reschedule(ctx, event.ID, event.ClaimToken, "timeout", deadline); !errors.Is(err, repository.ErrOutboxClaimFenced) {
					t.Fatalf("old publisher rescheduled: %v", err)
				}
			}
			var requests int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_type='image.processing.requested'").Scan(&requests); err != nil || requests != 0 {
				t.Fatal("pending requests survive", err)
			}
			if attempt.Job.AttemptToken != "" {
				if _, err := jobRepo(t, pool).Complete(ctx, job.ID, attempt.Job.AttemptToken, resultFor(attempt)); !errors.Is(err, repository.ErrAttemptFenced) {
					t.Fatalf("old attempt accepted: %v", err)
				}
			}
		})
	}
}

func mustStorageRows(t *testing.T, pool *pgxpool.Pool) pgx.Rows {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT * FROM storage_objects ORDER BY object_key")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestDeleteIdempotenceAndMissing(t *testing.T) {
	pool := migratedReadPool(t)
	job := seedJob(t, pool)
	ctx := context.Background()
	repo := repository.NewDeleteRepository(pool)
	if err := repo.Delete(ctx, uuid.NewString()); !errors.Is(err, domain.ErrImageNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if err := repo.Delete(ctx, job.ImageID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE images SET cleanup_after=clock_timestamp()+interval '1 hour',cleanup_token=$1,cleanup_lease_until=clock_timestamp()+interval '1 minute',last_cleanup_at=clock_timestamp()`, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	before := deleteSnapshot(t, pool)
	if err := repo.Delete(ctx, job.ImageID); err != nil {
		t.Fatal(err)
	}
	if before != deleteSnapshot(t, pool) {
		t.Fatal("repeat deletion reset cleanup or metadata")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := repo.Delete(ctx, job.ImageID); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cause lost: %v", err)
	}
}

func TestDeleteConcurrent(t *testing.T) {
	pool := migratedReadPool(t)
	job := seedJob(t, pool)
	repo := repository.NewDeleteRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { <-start; done <- repo.Delete(ctx, job.ImageID) }()
	}
	close(start)
	for i := 0; i < 8; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if got, err := jobRepo(t, pool).Claim(ctx, job.ID); err != nil || got.Disposition != repository.ClaimImageDeleted {
		t.Fatalf("deleted claim: %+v %v", got, err)
	}
}

// deleteLockBarrier pauses the first transaction after it owns the image lock.
type deleteLockBarrier struct {
	locked, release chan struct{}
	once            sync.Once
}
type deleteLockKey struct{}

func (b *deleteLockBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, deleteLockKey{}, strings.Contains(data.SQL, "FROM images WHERE id=$1 FOR UPDATE"))
}
func (b *deleteLockBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(deleteLockKey{}).(bool); match && data.Err == nil {
		b.once.Do(func() {
			close(b.locked)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestDeleteJobRaces(t *testing.T) {
	for _, operation := range []string{"claim", "complete", "retry", "fail"} {
		for _, deleteFirst := range []bool{true, false} {
			order := "job first"
			if deleteFirst {
				order = "delete first"
			}
			t.Run(operation+"/"+order, func(t *testing.T) {
				pool := migratedReadPool(t)
				job := seedJob(t, pool)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var attempt repository.ClaimResult
				if operation != "claim" {
					attempt = claimJob(t, pool, job.ID)
				}
				barrier := &deleteLockBarrier{locked: make(chan struct{}), release: make(chan struct{})}
				cfg := pool.Config()
				cfg.ConnConfig.Tracer = barrier
				first, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { cancel(); first.Close() }()
				cfg = pool.Config()
				app := cfg.ConnConfig.RuntimeParams["search_path"] + "_delete_wait"
				cfg.ConnConfig.RuntimeParams["application_name"] = app
				second, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { cancel(); second.Close() }()
				deletion := func(p *pgxpool.Pool) error { return repository.NewDeleteRepository(p).Delete(ctx, job.ImageID) }
				jobAction := func(p *pgxpool.Pool) error {
					repo := jobRepo(t, p)
					switch operation {
					case "claim":
						result, err := repo.Claim(ctx, job.ID)
						if err == nil {
							want := repository.ClaimAcquired
							if deleteFirst {
								want = repository.ClaimImageDeleted
							}
							if result.Disposition != want {
								return errors.New("unexpected claim disposition")
							}
						}
						return err
					case "complete":
						_, err := repo.Complete(ctx, job.ID, attempt.Job.AttemptToken, resultFor(attempt))
						return err
					case "retry":
						_, err := repo.Retry(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure, time.Now().Add(time.Minute))
						return err
					default:
						_, err := repo.Fail(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure)
						return err
					}
				}
				firstDone, secondDone := make(chan error, 1), make(chan error, 1)
				go func() {
					if deleteFirst {
						firstDone <- deletion(first)
					} else {
						firstDone <- jobAction(first)
					}
				}()
				select {
				case <-barrier.locked:
				case <-ctx.Done():
					t.Fatal("first image lock not reached")
				}
				go func() {
					if deleteFirst {
						secondDone <- jobAction(second)
					} else {
						secondDone <- deletion(second)
					}
				}()
				awaitOutboxState(t, ctx, func() (bool, error) {
					var waiting bool
					err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&waiting)
					return waiting, err
				})
				// This PostgreSQL timestamp must precede the deletion timestamp, despite the
				// deletion transaction having started before the competing lock was released.
				var lockedAt time.Time
				if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&lockedAt); err != nil {
					t.Fatal(err)
				}
				close(barrier.release)
				for i, ch := range []chan error{firstDone, secondDone} {
					select {
					case err := <-ch:
						fenced := deleteFirst && i == 1 && operation != "claim"
						if fenced {
							if !errors.Is(err, repository.ErrAttemptFenced) {
								t.Fatalf("attempt not fenced: %v", err)
							}
						} else if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("race did not finish")
					}
				}
				var status string
				var deleted time.Time
				if err := pool.QueryRow(ctx, "SELECT j.status,i.deleted_at FROM jobs j JOIN images i ON i.id=j.image_id WHERE j.id=$1", job.ID).Scan(&status, &deleted); err != nil {
					t.Fatal(err)
				}
				want := "cancelled"
				if !deleteFirst && operation == "complete" {
					want = "completed"
				}
				if !deleteFirst && operation == "fail" {
					want = "failed"
				}
				if status != want || deleted.Before(lockedAt) {
					t.Fatalf("race outcome: %s %v, want %s after %v", status, deleted, want, lockedAt)
				}
				var invalid int
				if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM storage_objects WHERE state<>'discarded')+
 (SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_type='image.processing.requested')`).Scan(&invalid); err != nil || invalid != 0 {
					t.Fatal("delete race left active storage or requests", err)
				}
			})
		}
	}
}

func TestDeleteRollback(t *testing.T) {
	for _, table := range []string{"images", "jobs", "storage_objects", "outbox"} {
		t.Run(table, func(t *testing.T) {
			pool := migratedReadPool(t)
			job := seedJob(t, pool)
			claimJob(t, pool, job.ID)
			claimOutbox(t, pool)
			ctx := context.Background()
			event := "UPDATE"
			if table == "outbox" {
				event = "DELETE"
			}
			if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected delete failure'; END $$;
 CREATE TRIGGER reject_delete AFTER `+event+` ON `+table+` FOR EACH STATEMENT EXECUTE FUNCTION reject_delete()`); err != nil {
				t.Fatal(err)
			}
			before := deleteSnapshot(t, pool)
			err := repository.NewDeleteRepository(pool).Delete(ctx, job.ImageID)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || errors.Is(err, repository.ErrDeleteCommitUncertain) {
				t.Fatalf("rollback cause: %v", err)
			}
			if before != deleteSnapshot(t, pool) {
				t.Fatal("partial deletion survived rollback")
			}
		})
	}
}

func TestDeleteUncertainCommit(t *testing.T) {
	pool := migratedReadPool(t)
	job := seedJob(t, pool)
	attempt := claimJob(t, pool, job.ID)
	event := claimOutbox(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocked, release := make(chan struct{}), make(chan struct{})
	cfg := pool.Config()
	dial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(dialCtx, network, address)
		if err != nil {
			return nil, err
		}
		return &uploadCommitResponseDrop{Conn: conn, blocked: blocked, release: release, ctx: ctx}, nil
	}
	client, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); client.Close() }()
	done := make(chan error, 1)
	go func() { done <- repository.NewDeleteRepository(client).Delete(ctx, job.ImageID) }()
	select {
	case <-blocked:
	case err := <-done:
		t.Fatalf("COMMIT not intercepted: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	awaitOutboxState(t, ctx, func() (bool, error) {
		var deleted bool
		err := pool.QueryRow(ctx, "SELECT deleted_at IS NOT NULL FROM images WHERE id=$1", job.ImageID).Scan(&deleted)
		return deleted, err
	})
	committed := deleteSnapshot(t, pool)
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrDeleteCommitUncertain) || !errors.Is(err, pgconn.ErrConnClosed) {
			t.Fatalf("uncertain commit lost cause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := repository.NewDeleteRepository(pool).Delete(ctx, job.ImageID); err != nil {
		t.Fatal(err)
	}
	if committed != deleteSnapshot(t, pool) {
		t.Fatal("retry after uncertain COMMIT changed cleanup state")
	}
	if _, err := jobRepo(t, pool).Fail(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure); !errors.Is(err, repository.ErrAttemptFenced) {
		t.Fatal("old attempt accepted", err)
	}
	if err := outboxRepo(t, pool).MarkPublished(ctx, event.ID, event.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
		t.Fatal("old publisher accepted", err)
	}
}

func TestDeleteMixedJobsAndIsolation(t *testing.T) {
	pool := migratedReadPool(t)
	ctx := context.Background()
	image := uploadInput()
	registerUpload(t, pool, image)
	created, err := uploadRepo(t, pool).Create(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	claimJob(t, pool, created.Jobs[1].ID)
	third := claimJob(t, pool, created.Jobs[2].ID)
	if _, err := jobRepo(t, pool).Retry(ctx, third.Job.ID, third.Job.AttemptToken, transientJobFailure, retryTime(t, pool)); err != nil {
		t.Fatal(err)
	}
	other := uploadInput()
	other.ID = uuid.NewString()
	other.OriginalKey = other.ID + "/original.png"
	other.Jobs = other.Jobs[:1]
	other.Jobs[0].ID = uuid.NewString()
	registerUpload(t, pool, other)
	untouched, err := uploadRepo(t, pool).Create(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	var otherStorage, otherOutbox string
	if err := pool.QueryRow(ctx, "SELECT to_jsonb(s)::text FROM storage_objects s WHERE image_id=$1", other.ID).Scan(&otherStorage); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT to_jsonb(o)::text FROM outbox o WHERE job_id=$1", other.Jobs[0].ID).Scan(&otherOutbox); err != nil {
		t.Fatal(err)
	}
	if err := repository.NewDeleteRepository(pool).Delete(ctx, image.ID); err != nil {
		t.Fatal(err)
	}
	var cancelled int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE image_id=$1 AND status='cancelled'", image.ID).Scan(&cancelled); err != nil || cancelled != 3 {
		t.Fatal("mixed jobs not all cancelled", err)
	}
	got, err := repository.NewImageRepository(pool).GetByID(ctx, other.ID)
	if err != nil || !reflect.DeepEqual(got, untouched) {
		t.Fatal("unrelated image or job changed", err)
	}
	var storage, event string
	if err := pool.QueryRow(ctx, "SELECT to_jsonb(s)::text FROM storage_objects s WHERE image_id=$1", other.ID).Scan(&storage); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT to_jsonb(o)::text FROM outbox o WHERE job_id=$1", other.Jobs[0].ID).Scan(&event); err != nil {
		t.Fatal(err)
	}
	if storage != otherStorage || event != otherOutbox {
		t.Fatal("unrelated storage or outbox changed")
	}
}
