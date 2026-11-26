//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	objectstorage "github.com/akhmed9505/image-processor/internal/infra/storage"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	processor "github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func serviceCleanupRepo(t *testing.T, p *pgxpool.Pool) *repository.CleanupRepository {
	t.Helper()
	r, e := repository.NewCleanupRepository(p, service.CleanupLease)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func runStorageCleanup(t *testing.T, r service.CleanupRepository, p *pgxpool.Pool, s service.CleanupStorage) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- service.NewCleanupService(r, repository.NewStorageRepository(p), s, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx)
	}()
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
				t.Error("cleanup not stopped")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}
func putCleanupObject(t *testing.T, s *objectstorage.Storage, key string) {
	t.Helper()
	if e := s.Put(context.Background(), key, bytes.NewReader([]byte("late put")), 8, "image/png"); e != nil {
		t.Fatal(e)
	}
}
func awaitCleanedImage(t *testing.T, p *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	awaitOutboxState(t, ctx, func() (bool, error) {
		var done bool
		e := p.QueryRow(ctx, "SELECT last_cleanup_at IS NOT NULL FROM images WHERE deleted_at IS NOT NULL").Scan(&done)
		return done, e
	})
}

func awaitCleanupKeys(t *testing.T, s *objectstorage.Storage, keys ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	awaitOutboxState(t, ctx, func() (bool, error) {
		for _, key := range keys {
			_, err := s.Stat(ctx, key)
			if errors.Is(err, objectstorage.ErrObjectNotFound) {
				continue
			}
			return false, err
		}
		return true, nil
	})
}

func TestCleanupServiceLatePUTAndProtection(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx := context.Background()
	_, a := admittedJob(t, p, s)
	if _, e := processor.NewJobService(processingRepo(t, p), s).ProcessAttempt(ctx, a); e != nil {
		t.Fatal(e)
	}
	if e := repository.NewDeleteRepository(p).Delete(ctx, a.Job.ImageID); e != nil {
		t.Fatal(e)
	}
	// Retain another active image and completed result, even after its write windows expire.
	_, active := admittedJob(t, p, s)
	if _, e := processor.NewJobService(processingRepo(t, p), s).ProcessAttempt(ctx, active); e != nil {
		t.Fatal(e)
	}
	_, failed := admittedJob(t, p, s)
	if _, e := processor.NewJobService(processingRepo(t, p), processingStorage{s, func(context.Context, string) error { return io.ErrUnexpectedEOF }}).ProcessAttempt(ctx, failed); !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal(e)
	}
	_, live := admittedJob(t, p, s)
	putCleanupObject(t, s, live.ResultKey)
	registry := repository.NewStorageRepository(p)
	orphan := uuid.NewString() + "/original.png"
	if _, e := registry.RegisterOriginal(ctx, orphan, uuid.NewString(), time.Minute); e != nil {
		t.Fatal(e)
	}
	putCleanupObject(t, s, orphan)
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '11 minutes'; UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second' WHERE deleted_at IS NOT NULL"); e != nil {
		t.Fatal(e)
	}
	protected := []string{}
	for _, discard := range []bool{false, true} {
		key := uuid.NewString() + "/original.png"
		if _, e := registry.RegisterOriginal(ctx, key, uuid.NewString(), time.Minute); e != nil {
			t.Fatal(e)
		}
		if discard {
			if _, e := registry.DiscardOriginal(ctx, key); e != nil {
				t.Fatal(e)
			}
		}
		putCleanupObject(t, s, key)
		protected = append(protected, key)
	}
	graceKey := uuid.NewString() + "/original.png"
	if _, e := registry.RegisterOriginal(ctx, graceKey, uuid.NewString(), time.Minute); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '5 minutes' WHERE object_key=$1", graceKey); e != nil {
		t.Fatal(e)
	}
	putCleanupObject(t, s, graceKey)
	protected = append(protected, graceKey)
	r := serviceCleanupRepo(t, p)
	stop1 := runStorageCleanup(t, r, p, s)
	stop2 := runStorageCleanup(t, r, p, s)
	awaitCleanedImage(t, p)
	awaitCleanupKeys(t, s, a.OriginalKey, a.ResultKey, orphan, failed.ResultKey)
	stop1()
	stop2()
	for _, key := range []string{a.OriginalKey, a.ResultKey, orphan, failed.ResultKey} {
		if _, e := s.Stat(ctx, key); !errors.Is(e, objectstorage.ErrObjectNotFound) {
			t.Fatal("object not cleaned", key, e)
		}
		if _, e := registry.GetByKey(ctx, key); e != nil {
			t.Fatal("registry removed", e)
		}
	}
	for _, key := range append(protected, active.OriginalKey, active.ResultKey, failed.OriginalKey, live.OriginalKey, live.ResultKey) {
		if _, e := s.Stat(ctx, key); e != nil {
			t.Fatal("protected object removed", key, e)
		}
	}
	for _, key := range []string{a.OriginalKey, a.ResultKey, orphan, failed.ResultKey} {
		putCleanupObject(t, s, key)
	}
	if _, e := p.Exec(ctx, "UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second',last_cleanup_at=NULL WHERE deleted_at IS NOT NULL"); e != nil {
		t.Fatal(e)
	}
	stop := runStorageCleanup(t, r, p, s)
	awaitCleanedImage(t, p)
	awaitCleanupKeys(t, s, a.OriginalKey, a.ResultKey, orphan, failed.ResultKey)
	stop()
	for _, key := range []string{a.OriginalKey, a.ResultKey, orphan, failed.ResultKey} {
		if _, e := s.Stat(ctx, key); !errors.Is(e, objectstorage.ErrObjectNotFound) {
			t.Fatal("late PUT survived", key, e)
		}
	}
}

type cleanupStorageFailure struct {
	*objectstorage.Storage
	key string
}

func (s cleanupStorageFailure) Delete(ctx context.Context, key string) error {
	if key == s.key {
		return io.ErrUnexpectedEOF
	}
	return s.Storage.Delete(ctx, key)
}
func TestCleanupServicePartialFailure(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, a := admittedJob(t, p, s)
	if e := repository.NewDeleteRepository(p).Delete(ctx, a.Job.ImageID); e != nil {
		t.Fatal(e)
	}
	orphan := uuid.NewString() + "/original.png"
	if _, e := repository.NewStorageRepository(p).RegisterOriginal(ctx, orphan, uuid.NewString(), time.Minute); e != nil {
		t.Fatal(e)
	}
	putCleanupObject(t, s, orphan)
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '11 minutes';UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	r := serviceCleanupRepo(t, p)
	stop := runStorageCleanup(t, r, p, cleanupStorageFailure{s, a.OriginalKey})
	awaitOutboxState(t, ctx, func() (bool, error) {
		var ready bool
		e := p.QueryRow(ctx, "SELECT cleanup_after>clock_timestamp() AND cleanup_token IS NULL AND last_cleanup_at IS NULL FROM images").Scan(&ready)
		return ready, e
	})
	awaitCleanupKeys(t, s, orphan)
	stop()
	if _, e := s.Stat(ctx, a.OriginalKey); e != nil {
		t.Fatal("failed delete removed object", e)
	}
	if _, e := s.Stat(ctx, orphan); !errors.Is(e, objectstorage.ErrObjectNotFound) {
		t.Fatal("partial failure stopped sweep", e)
	}
	if _, e := p.Exec(ctx, "UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	stop = runStorageCleanup(t, r, p, s)
	awaitCleanedImage(t, p)
	stop()
	if _, e := s.Stat(ctx, a.OriginalKey); !errors.Is(e, objectstorage.ErrObjectNotFound) {
		t.Fatal(e)
	}
}

type cleanupUncertainFind struct {
	*repository.CleanupRepository
	uncertain *repository.CleanupRepository
	failed    chan error
}

func (r cleanupUncertainFind) FindDeletable(ctx context.Context, n int, key string, grace time.Duration) (service.CleanupPage, error) {
	page, e := r.uncertain.FindDeletable(ctx, n, key, grace)
	r.failed <- e
	return page, e
}
func TestCleanupServiceUncertainAuthorization(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := uuid.NewString() + "/original.png"
	if _, e := repository.NewStorageRepository(p).RegisterOriginal(ctx, key, uuid.NewString(), time.Minute); e != nil {
		t.Fatal(e)
	}
	putCleanupObject(t, s, key)
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '11 minutes'"); e != nil {
		t.Fatal(e)
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
	stop := runStorageCleanup(t, cleanupUncertainFind{serviceCleanupRepo(t, p), serviceCleanupRepo(t, client), failed}, p, s)
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	awaitOutboxState(t, ctx, func() (bool, error) {
		var discarded bool
		e := p.QueryRow(ctx, "SELECT state='discarded' FROM storage_objects WHERE object_key=$1", key).Scan(&discarded)
		return discarded, e
	})
	close(release)
	select {
	case e := <-failed:
		if !errors.Is(e, service.ErrCleanupCommitUncertain) || !errors.Is(e, pgconn.ErrConnClosed) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if _, e := s.Stat(ctx, key); e != nil {
		t.Fatal("deleted on uncertain authorization", e)
	}
	stop = runStorageCleanup(t, serviceCleanupRepo(t, p), p, s)
	awaitOutboxState(t, ctx, func() (bool, error) {
		_, e := s.Stat(ctx, key)
		if errors.Is(e, objectstorage.ErrObjectNotFound) {
			return true, nil
		}
		return false, e
	})
	stop()
}

type scopedCleanupOnly struct {
	*repository.CleanupRepository
	claimed chan service.CleanupClaim
}

func (r scopedCleanupOnly) Claim(ctx context.Context, n int) ([]service.CleanupClaim, error) {
	claims, e := r.CleanupRepository.Claim(ctx, n)
	if e == nil && len(claims) > 0 {
		r.claimed <- claims[0]
	}
	return claims, e
}
func (r scopedCleanupOnly) FindDeletable(context.Context, int, string, time.Duration) (service.CleanupPage, error) {
	return service.CleanupPage{}, nil
}

func TestCleanupServiceSeparatesTombstones(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originals := make(map[string]string)
	for range 2 {
		_, a := admittedJob(t, p, s)
		originals[a.Job.ImageID] = a.OriginalKey
		if e := repository.NewDeleteRepository(p).Delete(ctx, a.Job.ImageID); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '11 minutes'; UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	r := serviceCleanupRepo(t, p)
	claims := make(chan service.CleanupClaim, 1)
	completed := make(map[string]bool)
	for range 2 {
		stop := runStorageCleanup(t, scopedCleanupOnly{r, claims}, p, s)
		var claim service.CleanupClaim
		select {
		case claim = <-claims:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if completed[claim.ImageID] {
			t.Fatal("wrong tombstone claimed again")
		}
		awaitOutboxState(t, ctx, func() (bool, error) {
			var done bool
			e := p.QueryRow(ctx, "SELECT last_cleanup_at IS NOT NULL FROM images WHERE id=$1", claim.ImageID).Scan(&done)
			return done, e
		})
		stop()
		completed[claim.ImageID] = true
		for id, key := range originals {
			_, e := s.Stat(ctx, key)
			if completed[id] {
				if !errors.Is(e, objectstorage.ErrObjectNotFound) {
					t.Fatal("claimed image not cleaned", id, e)
				}
			} else {
				if e != nil {
					t.Fatal("foreign image cleaned under another claim", id, e)
				}
				var done bool
				if e := p.QueryRow(ctx, "SELECT last_cleanup_at IS NOT NULL FROM images WHERE id=$1", id).Scan(&done); e != nil || done {
					t.Fatal("foreign cleanup marked successful", id, e)
				}
			}
		}
	}
}

func TestCleanupScopedAuthorizationFencing(t *testing.T) {
	p := migratedReadPool(t)
	ctx := context.Background()
	job := seedJob(t, p)
	if e := repository.NewDeleteRepository(p).Delete(ctx, job.ImageID); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '11 minutes'; UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	r := serviceCleanupRepo(t, p)
	claims, e := r.Claim(ctx, 1)
	if e != nil || len(claims) != 1 {
		t.Fatal(claims, e)
	}
	claim := claims[0]
	wrong := claim
	wrong.Token = uuid.NewString()
	if page, e := r.FindDeletableForImage(ctx, 100, "", service.CleanupGrace, wrong); !errors.Is(e, repository.ErrCleanupFenced) || len(page.Objects) > 0 {
		t.Fatal("wrong token authorized deletion", page, e)
	}
	if _, e := p.Exec(ctx, "UPDATE images SET cleanup_lease_until=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	if page, e := r.FindDeletableForImage(ctx, 100, "", service.CleanupGrace, claim); !errors.Is(e, repository.ErrCleanupFenced) || len(page.Objects) > 0 {
		t.Fatal("expired lease authorized deletion", page, e)
	}
}
