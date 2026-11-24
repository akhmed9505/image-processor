//go:build integration

package integration

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStorageRegisterAndGet(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx := context.Background()
	var before, after time.Time
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	object, err := repo.RegisterOriginal(ctx, "1/original.png", readImageID(1), 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if object.ObjectKey != "1/original.png" || object.ImageID != readImageID(1) || object.JobID != "" || object.AttemptToken != "" || object.State != repository.StorageStateWriting {
		t.Fatalf("unexpected intent: %+v", object)
	}
	if object.WriteUntil.Before(before.Add(2*time.Minute)) || object.WriteUntil.After(after.Add(2*time.Minute)) || object.CreatedAt.IsZero() || object.UpdatedAt.IsZero() {
		t.Fatal("deadline was not calculated using database time")
	}
	got, err := repo.GetByKey(ctx, object.ObjectKey)
	if err != nil || !reflect.DeepEqual(got, object) {
		t.Fatalf("GetByKey: got %+v, %v, want %+v", got, err, object)
	}
	var images int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM images").Scan(&images); err != nil || images != 0 {
		t.Fatalf("intent must not require or create an image: count=%d error=%v", images, err)
	}
	for _, state := range []repository.StorageState{repository.StorageStateWriting, repository.StorageStateDiscarded} {
		if state == repository.StorageStateDiscarded {
			object, err = repo.DiscardOriginal(ctx, object.ObjectKey)
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := repo.RegisterOriginal(ctx, object.ObjectKey, readImageID(2), time.Hour); !errors.Is(err, repository.ErrStorageObjectExists) {
			t.Fatalf("duplicate: %v", err)
		}
		got, err := repo.GetByKey(ctx, object.ObjectKey)
		if err != nil || !reflect.DeepEqual(got, object) {
			t.Fatal("registration overwrote existing intent")
		}
	}
}

func TestStorageConcurrentRegistration(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		object repository.StorageObject
		err    error
	}
	done := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() {
			<-start
			object, err := repo.RegisterOriginal(ctx, "contested-original", readImageID(i+1), time.Duration(i+1)*time.Minute)
			done <- result{object, err}
		}()
	}
	close(start)
	successes := 0
	var winner repository.StorageObject
	for i := 0; i < 8; i++ {
		select {
		case result := <-done:
			if result.err == nil {
				successes++
				winner = result.object
			} else if !errors.Is(result.err, repository.ErrStorageObjectExists) {
				t.Fatal(result.err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent registration timed out")
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful registrations, want one", successes)
	}
	got, err := repo.GetByKey(ctx, "contested-original")
	if err != nil || !reflect.DeepEqual(got, winner) {
		t.Fatal("winning intent was overwritten")
	}
}

func TestStorageDiscard(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx := context.Background()
	for _, state := range []repository.StorageState{repository.StorageStateWriting, repository.StorageStateRetained, repository.StorageStateDiscarded} {
		t.Run(string(state), func(t *testing.T) {
			key := string(state)
			if _, err := repo.RegisterOriginal(ctx, key, readImageID(1), time.Minute); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "UPDATE storage_objects SET state=$2 WHERE object_key=$1", key, state); err != nil {
				t.Fatal(err)
			}
			before, err := repo.GetByKey(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			got, err := repo.DiscardOriginal(ctx, key)
			if state == repository.StorageStateWriting {
				if err != nil {
					t.Fatal(err)
				}
				if got.State != repository.StorageStateDiscarded || got.ObjectKey != before.ObjectKey || got.ImageID != before.ImageID || !got.CreatedAt.Equal(before.CreatedAt) || !got.WriteUntil.Equal(before.WriteUntil) || got.UpdatedAt.Before(before.UpdatedAt) {
					t.Fatalf("invalid discard result: %+v", got)
				}
			} else if !errors.Is(err, repository.ErrInvalidStorageTransition) {
				t.Fatalf("expected invalid transition, got %v", err)
			}
			stored, readErr := repo.GetByKey(ctx, key)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if state == repository.StorageStateWriting && !reflect.DeepEqual(stored, got) {
				t.Fatal("discard result differs from stored record")
			}
			if state != repository.StorageStateWriting && !reflect.DeepEqual(before, stored) {
				t.Fatal("rejected discard changed the record")
			}
		})
	}
}

func TestStorageConcurrentDiscard(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := repo.RegisterOriginal(ctx, "contested", readImageID(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { <-start; _, err := repo.DiscardOriginal(ctx, "contested"); done <- err }()
	}
	close(start)
	successes := 0
	for i := 0; i < 8; i++ {
		select {
		case err := <-done:
			if err == nil {
				successes++
			} else if !errors.Is(err, repository.ErrInvalidStorageTransition) {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent discard timed out")
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful discards, want one", successes)
	}
	stored, err := repo.GetByKey(ctx, "contested")
	if err != nil || stored.State != repository.StorageStateDiscarded {
		t.Fatalf("unexpected final record: %+v, %v", stored, err)
	}
}

func TestStorageOriginalOnly(t *testing.T) {
	pool := migratedReadPool(t)
	insertReadImage(t, pool, 1, readCreatedAt, false)
	jobs := insertReadJobs(t, pool)
	job := jobs[1]
	if _, err := pool.Exec(context.Background(), `INSERT INTO storage_objects(object_key,image_id,job_id,attempt_token,write_until)
 VALUES ('job-result',$1,$2,$3,clock_timestamp()+interval '1 minute')`, job.ImageID, job.ID, job.AttemptToken); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewStorageRepository(pool)
	object, err := repo.GetByKey(context.Background(), "job-result")
	if err != nil || object.JobID != job.ID || object.AttemptToken != job.AttemptToken {
		t.Fatalf("job registry mapping: %+v, %v", object, err)
	}
	if _, err := repo.DiscardOriginal(context.Background(), object.ObjectKey); !errors.Is(err, repository.ErrInvalidStorageTransition) {
		t.Fatalf("job intent discard: %v", err)
	}
	stored, err := repo.GetByKey(context.Background(), object.ObjectKey)
	if err != nil || !reflect.DeepEqual(stored, object) {
		t.Fatal("job-owned intent was modified")
	}
}

func TestStorageErrors(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx := context.Background()
	if _, err := repo.GetByKey(ctx, "missing"); !errors.Is(err, repository.ErrStorageObjectNotFound) {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := repo.DiscardOriginal(ctx, "missing"); !errors.Is(err, repository.ErrStorageObjectNotFound) {
		t.Fatalf("missing discard key: %v", err)
	}
	for _, ttl := range []time.Duration{-1, 0, time.Nanosecond} {
		if _, err := repo.RegisterOriginal(ctx, "invalid", readImageID(1), ttl); !errors.Is(err, repository.ErrInvalidWriteIntent) {
			t.Fatalf("invalid TTL: %v", err)
		}
	}
	if _, err := repo.RegisterOriginal(ctx, "", readImageID(1), time.Minute); !errors.Is(err, repository.ErrInvalidWriteIntent) {
		t.Fatalf("empty key: %v", err)
	}
	_, err := repo.RegisterOriginal(ctx, "invalid-uuid", "invalid", time.Minute)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22P02" {
		t.Fatalf("expected wrapped UUID error, got %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.GetByKey(canceled, "anything"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := repo.RegisterOriginal(canceled, "anything", readImageID(1), time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("register cancellation: %v", err)
	}
	if _, err := repo.DiscardOriginal(canceled, "anything"); !errors.Is(err, context.Canceled) {
		t.Fatalf("discard cancellation: %v", err)
	}
}

func TestStorageDiscardAfterConcurrentRetention(t *testing.T) {
	pool := migratedReadPool(t)
	repo := repository.NewStorageRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retained, err := repo.RegisterOriginal(ctx, "locked", readImageID(1), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(ctx, "SELECT object_key FROM storage_objects WHERE object_key='locked' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	application := cfg.ConnConfig.RuntimeParams["search_path"] + "_storage_wait"
	cfg.ConnConfig.RuntimeParams["application_name"] = application
	waiter, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); locker.Rollback(context.Background()); waiter.Close() }()
	done := make(chan error, 1)
	go func() {
		_, err := repository.NewStorageRepository(waiter).DiscardOriginal(ctx, "locked")
		done <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var waiting int
	for {
		err := pool.QueryRow(ctx, `SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'`, application).Scan(&waiting)
		if err == nil {
			break
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("discard did not wait for the row lock")
		}
	}
	// Simulate the future upload transaction retaining the object while discard waits.
	if err := locker.QueryRow(ctx, "UPDATE storage_objects SET state='retained',updated_at=clock_timestamp() WHERE object_key='locked' RETURNING updated_at").Scan(&retained.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	retained.State = repository.StorageStateRetained
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrInvalidStorageTransition) {
			t.Fatalf("expected retained object rejection after lock, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("discard did not finish")
	}
	stored, err := repo.GetByKey(ctx, "locked")
	if err != nil || !reflect.DeepEqual(stored, retained) {
		t.Fatalf("retained intent was modified: %+v, %v", stored, err)
	}
}
