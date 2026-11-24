//go:build integration

package integration

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func cleanupRepo(t *testing.T, p *pgxpool.Pool) *repository.CleanupRepository {
	t.Helper()
	r, e := repository.NewCleanupRepository(p, 30*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func expireAttempt(t *testing.T, p *pgxpool.Pool) {
	t.Helper()
	if _, e := p.Exec(context.Background(), "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE status='processing'"); e != nil {
		t.Fatal(e)
	}
}

func TestRecoveryExpiredAttempts(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		name := "retry"
		if exhausted {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			p := migratedReadPool(t)
			j := seedJob(t, p)
			attempt := claimJob(t, p, j.ID)
			ctx := context.Background()
			expireAttempt(t, p)
			if exhausted {
				if _, e := p.Exec(ctx, "UPDATE jobs SET attempts=2"); e != nil {
					t.Fatal(e)
				}
			}
			r := repository.NewRecoveryRepository(jobRepo(t, p))
			var policy func(int) time.Duration
			if !exhausted {
				policy = func(int) time.Duration { return time.Minute }
			}
			ids, e := r.Recover(ctx, 1, policy)
			if e != nil || len(ids) != 1 || ids[0] != j.ID {
				t.Fatalf("recover %v %v", ids, e)
			}
			got := storedJob(t, p)
			want := domain.JobStatusRetrying
			if exhausted {
				want = domain.JobStatusFailed
			}
			if got.Status != want || got.AttemptToken != "" || !got.LeaseUntil.IsZero() || got.LastErrorCode != "lease_expired" {
				t.Fatalf("recovered job %+v", got)
			}
			obj, e := repository.NewStorageRepository(p).GetByKey(ctx, attempt.ResultKey)
			if e != nil || obj.State != repository.StorageStateDiscarded || !obj.WriteUntil.Equal(attempt.Job.LeaseUntil) {
				t.Fatal("intent not safely discarded", e)
			}
			if exhausted {
				assertJobDLQ(t, p, got)
			} else if !got.NextRetryAt.Equal(got.UpdatedAt.Add(time.Minute)) {
				t.Fatal("retry time lost")
			}
			if _, e := jobRepo(t, p).Complete(ctx, j.ID, attempt.Job.AttemptToken, resultFor(attempt)); !errors.Is(e, repository.ErrAttemptFenced) {
				t.Fatal("old attempt accepted", e)
			}
			ids, e = r.Recover(ctx, 1, func(int) time.Duration { return time.Minute })
			if e != nil || len(ids) != 0 {
				t.Fatal("recovered twice", e)
			}
		})
	}
}

func TestRecoveryRedispatch(t *testing.T) {
	p := migratedReadPool(t)
	j := seedJob(t, p)
	ctx := context.Background()
	r := repository.NewRecoveryRepository(jobRepo(t, p))
	ids, e := r.Redispatch(ctx, 1)
	if e != nil || len(ids) != 0 {
		t.Fatal("early redispatch", e)
	}
	event := claimOutbox(t, p)
	redispatch := func() {
		t.Helper()
		if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second'"); e != nil {
			t.Fatal(e)
		}
		ids, e := r.Redispatch(ctx, 1)
		if e != nil || len(ids) != 1 {
			t.Fatalf("redispatch %v %v", ids, e)
		}
	}
	before := outboxSnapshot(t, p)
	for i := 0; i < 3; i++ {
		redispatch()
	}
	if before != outboxSnapshot(t, p) {
		t.Fatal("redispatch changed active publisher claim")
	}
	at := retryTime(t, p)
	if e := outboxRepo(t, p).Reschedule(ctx, event.ID, event.ClaimToken, "kafka_unavailable", at); e != nil {
		t.Fatal("publisher claim invalidated", e)
	}
	before = outboxSnapshot(t, p)
	for i := 0; i < 3; i++ {
		redispatch()
	}
	if before != outboxSnapshot(t, p) {
		t.Fatal("redispatch reset publication backoff")
	}
	if _, e := p.Exec(ctx, "UPDATE outbox SET available_at=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	fresh := claimOutbox(t, p)
	if e := outboxRepo(t, p).MarkPublished(ctx, fresh.ID, fresh.ClaimToken); e != nil {
		t.Fatal(e)
	}
	// A published notification is replaced by one new durable event.
	if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	ids, e = r.Redispatch(ctx, 1)
	if e != nil || len(ids) != 1 || ids[0] != j.ID {
		t.Fatal(e)
	}
	if e := repository.NewDeleteRepository(p).Delete(ctx, j.ImageID); e != nil {
		t.Fatal(e)
	}
	ids, e = r.Redispatch(ctx, 1)
	if e != nil || len(ids) != 0 {
		t.Fatal("deleted job redispatched", e)
	}
}

func TestRecoveryConcurrentBatch(t *testing.T) {
	p := migratedReadPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 6; i++ {
		image := uploadInput()
		image.ID = uuid.NewString()
		image.OriginalKey = image.ID + "/original.png"
		image.Jobs = image.Jobs[:1]
		image.Jobs[0].ID = uuid.NewString()
		registerUpload(t, p, image)
		stored, e := uploadRepo(t, p).Create(ctx, image)
		if e != nil {
			t.Fatal(e)
		}
		claimJob(t, p, stored.Jobs[0].ID)
	}
	expireAttempt(t, p)
	r := repository.NewRecoveryRepository(jobRepo(t, p))
	type result struct {
		ids []string
		e   error
	}
	done := make(chan result, 3)
	start := make(chan struct{})
	for i := 0; i < 3; i++ {
		go func() {
			<-start
			ids, e := r.Recover(ctx, 2, func(int) time.Duration { return time.Minute })
			done <- result{ids, e}
		}()
	}
	close(start)
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case got := <-done:
			if got.e != nil || len(got.ids) != 2 {
				t.Fatalf("batch %v %v", got.ids, got.e)
			}
			for _, id := range got.ids {
				if seen[id] {
					t.Fatal("duplicate recovery")
				}
				seen[id] = true
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func deletedCleanupImage(t *testing.T, p *pgxpool.Pool) string {
	t.Helper()
	j := seedJob(t, p)
	if e := repository.NewDeleteRepository(p).Delete(context.Background(), j.ImageID); e != nil {
		t.Fatal(e)
	}
	return j.ImageID
}
func claimCleanup(t *testing.T, p *pgxpool.Pool) repository.CleanupClaim {
	t.Helper()
	c, e := cleanupRepo(t, p).Claim(context.Background(), 1)
	if e != nil || len(c) != 1 {
		t.Fatalf("cleanup claim %v %v", c, e)
	}
	return c[0]
}

func TestCleanupPassesAndFencing(t *testing.T) {
	p := migratedReadPool(t)
	id := deletedCleanupImage(t, p)
	ctx := context.Background()
	r := cleanupRepo(t, p)
	first := claimCleanup(t, p)
	c, e := r.Claim(ctx, 1)
	if e != nil || len(c) != 0 {
		t.Fatal("active claim stolen", e)
	}
	if e := r.Complete(ctx, id, uuid.NewString(), retryTime(t, p)); !errors.Is(e, repository.ErrCleanupFenced) {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE images SET cleanup_lease_until=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	if e := r.Reschedule(ctx, id, first.Token, retryTime(t, p)); !errors.Is(e, repository.ErrCleanupFenced) {
		t.Fatal("expired lease accepted", e)
	}
	second := claimCleanup(t, p)
	if second.Token == first.Token {
		t.Fatal("token reused")
	}
	if e := r.Complete(ctx, id, first.Token, retryTime(t, p)); !errors.Is(e, repository.ErrCleanupFenced) {
		t.Fatal("stale token accepted", e)
	}
	at := retryTime(t, p)
	if e := r.Complete(ctx, id, second.Token, at); e != nil {
		t.Fatal(e)
	}
	var last, next time.Time
	var released bool
	if e := p.QueryRow(ctx, "SELECT last_cleanup_at,cleanup_after,cleanup_token IS NULL AND cleanup_lease_until IS NULL FROM images").Scan(&last, &next, &released); e != nil || last.IsZero() || !next.Equal(at) || !released {
		t.Fatal("pass not recorded", e)
	}
	c, e = r.Claim(ctx, 1)
	if e != nil || len(c) != 0 {
		t.Fatal("future pass claimed", e)
	}
	if _, e := p.Exec(ctx, "UPDATE images SET cleanup_after=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	third := claimCleanup(t, p)
	if e := r.Reschedule(ctx, id, third.Token, at); e != nil {
		t.Fatal(e)
	}
	var after time.Time
	if e := p.QueryRow(ctx, "SELECT last_cleanup_at FROM images").Scan(&after); e != nil || !last.Equal(after) {
		t.Fatal("failed pass changed last success", e)
	}
}

func TestCleanupSafeObjects(t *testing.T) {
	p := migratedReadPool(t)
	j := seedJob(t, p)
	ctx := context.Background()
	attempt := claimJob(t, p, j.ID)
	// An expired object deadline alone must not override a live processing attempt.
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '2 hours'"); e != nil {
		t.Fatal(e)
	}
	r := cleanupRepo(t, p)
	objects, e := r.FindDeletable(ctx, 10, "", time.Minute)
	if e != nil || len(objects.Objects) != 0 {
		t.Fatal("active original or attempt exposed", e)
	}
	// Complete needs a live intent; then both active references must protect outputs.
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()+interval '1 minute' WHERE job_id IS NOT NULL"); e != nil {
		t.Fatal(e)
	}
	if _, e := jobRepo(t, p).Complete(ctx, j.ID, attempt.Job.AttemptToken, resultFor(attempt)); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '2 hours'"); e != nil {
		t.Fatal(e)
	}
	objects, e = r.FindDeletable(ctx, 10, "", time.Minute)
	if e != nil || len(objects.Objects) != 0 {
		t.Fatal("active completed output exposed", e)
	}
	// An orphan with no image must not starve behind protected registry records.
	if _, e := repository.NewStorageRepository(p).RegisterOriginal(ctx, "orphan", uuid.NewString(), time.Minute); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 hour' WHERE object_key='orphan'"); e != nil {
		t.Fatal(e)
	}
	objects, e = r.FindDeletable(ctx, 1, "", time.Minute)
	if e != nil || len(objects.Objects) != 1 || objects.Objects[0].ObjectKey != "orphan" {
		t.Fatalf("orphan %v %v", objects, e)
	}
	if e := repository.NewDeleteRepository(p).Delete(ctx, j.ImageID); e != nil {
		t.Fatal(e)
	}
	// Discarded does not waive write_until or the additional grace interval.
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()+interval '1 minute'"); e != nil {
		t.Fatal(e)
	}
	objects, e = r.FindDeletable(ctx, 10, "", time.Minute)
	if e != nil || len(objects.Objects) != 0 {
		t.Fatal("live discarded write intent exposed", e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '30 seconds'"); e != nil {
		t.Fatal(e)
	}
	objects, e = r.FindDeletable(ctx, 10, "", time.Minute)
	if e != nil || len(objects.Objects) != 0 {
		t.Fatal("grace ignored", e)
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '2 minutes'"); e != nil {
		t.Fatal(e)
	}
	for pass := 0; pass < 2; pass++ {
		objects, e = r.FindDeletable(ctx, 10, "", time.Minute)
		if e != nil || len(objects.Objects) != 3 {
			t.Fatalf("late PUT pass %d: %v %v", pass, objects, e)
		}
	}
}

func TestCleanupConcurrentClaim(t *testing.T) {
	p := migratedReadPool(t)
	deletedCleanupImage(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := cleanupRepo(t, p)
	type result struct {
		c []repository.CleanupClaim
		e error
	}
	done := make(chan result, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() { <-start; c, e := r.Claim(ctx, 1); done <- result{c, e} }()
	}
	close(start)
	count := 0
	for i := 0; i < 8; i++ {
		select {
		case got := <-done:
			if got.e != nil {
				t.Fatal(got.e)
			}
			count += len(got.c)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if count != 1 {
		t.Fatalf("claims %d", count)
	}
}

func TestMaintenanceRollbackAndCommitUncertainty(t *testing.T) {
	for _, operation := range []string{"recover", "redispatch", "claim", "complete", "reschedule", "objects"} {
		for _, uncertain := range []bool{false, true} {
			name := "rollback"
			if uncertain {
				name = "uncertain"
			}
			t.Run(operation+"/"+name, func(t *testing.T) {
				p := migratedReadPool(t)
				j := seedJob(t, p)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var claim repository.CleanupClaim
				table, event := "images", "UPDATE"
				switch operation {
				case "recover":
					claimJob(t, p, j.ID)
					expireAttempt(t, p)
					table = "outbox"
				case "redispatch":
					if _, e := p.Exec(ctx, "UPDATE jobs SET next_dispatch_at=clock_timestamp()-interval '1 second'"); e != nil {
						t.Fatal(e)
					}
					table = "outbox"
				case "claim", "complete", "reschedule", "objects":
					if e := repository.NewDeleteRepository(p).Delete(ctx, j.ImageID); e != nil {
						t.Fatal(e)
					}
					if operation == "complete" || operation == "reschedule" {
						claim = claimCleanup(t, p)
					}
					if operation == "objects" {
						table = "storage_objects"
						if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 hour'"); e != nil {
							t.Fatal(e)
						}
					}
				}
				before := deleteSnapshot(t, p)
				at := retryTime(t, p)
				call := func(client *pgxpool.Pool) error {
					recovery := repository.NewRecoveryRepository(jobRepo(t, client))
					cleanup := cleanupRepo(t, client)
					switch operation {
					case "recover":
						ids, e := recovery.Recover(ctx, 1, func(int) time.Duration { return time.Minute })
						if e != nil && len(ids) != 0 {
							t.Error("failed recovery returned IDs")
						}
						return e
					case "redispatch":
						ids, e := recovery.Redispatch(ctx, 1)
						if e != nil && len(ids) != 0 {
							t.Error("failed redispatch returned IDs")
						}
						return e
					case "claim":
						c, e := cleanup.Claim(ctx, 1)
						if e != nil && len(c) != 0 {
							t.Error("failed claim returned ownership")
						}
						return e
					case "complete":
						return cleanup.Complete(ctx, j.ImageID, claim.Token, at)
					case "reschedule":
						return cleanup.Reschedule(ctx, j.ImageID, claim.Token, at)
					default:
						objects, e := cleanup.FindDeletable(ctx, 10, "", 0)
						if e != nil && len(objects.Objects) != 0 {
							t.Error("failed cleanup authorized deletion")
						}
						return e
					}
				}
				if !uncertain {
					if table == "outbox" {
						event = "INSERT OR UPDATE"
					}
					if _, e := p.Exec(ctx, `CREATE FUNCTION reject_maintenance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected maintenance failure'; END $$;
 CREATE TRIGGER reject_maintenance AFTER `+event+` ON `+table+` FOR EACH STATEMENT EXECUTE FUNCTION reject_maintenance()`); e != nil {
						t.Fatal(e)
					}
					e := call(p)
					var pgErr *pgconn.PgError
					if !errors.As(e, &pgErr) || pgErr.Code != "P0001" {
						t.Fatalf("lost cause %v", e)
					}
					if before != deleteSnapshot(t, p) {
						t.Fatal("rollback changed state")
					}
					return
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
				done := make(chan error, 1)
				go func() { done <- call(client) }()
				select {
				case <-blocked:
				case e := <-done:
					t.Fatalf("COMMIT not intercepted: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				awaitOutboxState(t, ctx, func() (bool, error) { return deleteSnapshot(t, p) != before, nil })
				committed := deleteSnapshot(t, p)
				close(release)
				marker := repository.ErrCleanupCommitUncertain
				if operation == "recover" || operation == "redispatch" {
					marker = repository.ErrJobCommitUncertain
				}
				select {
				case e := <-done:
					if !errors.Is(e, marker) || !errors.Is(e, pgconn.ErrConnClosed) {
						t.Fatalf("commit cause %v", e)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if committed != deleteSnapshot(t, p) {
					t.Fatal("uncertain COMMIT compensated changes")
				}
			})
		}
	}
}

func TestCleanupObjectPagination(t *testing.T) {
	p := migratedReadPool(t)
	ctx := context.Background()
	r := cleanupRepo(t, p)
	for _, key := range []string{"A", "a-", "a/", "z", "é", "я"} {
		if _, e := repository.NewStorageRepository(p).RegisterOriginal(ctx, key, uuid.NewString(), time.Minute); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 hour'"); e != nil {
		t.Fatal(e)
	}
	for pass := 0; pass < 2; pass++ {
		after := ""
		for _, key := range []string{"A", "a-", "a/", "z", "é", "я"} {
			objects, e := r.FindDeletable(ctx, 1, after, 0)
			if e != nil || len(objects.Objects) != 1 || objects.Objects[0].ObjectKey != key {
				t.Fatalf("pass %d cursor %s: %v %v", pass, after, objects, e)
			}
			after = objects.NextCursor
		}
		objects, e := r.FindDeletable(ctx, 1, after, 0)
		if e != nil || len(objects.Objects) != 0 || objects.NextCursor != "" {
			t.Fatal("page end", e)
		}
	}
}

func TestCleanupLeaseExpiresAfterLock(t *testing.T) {
	p := migratedReadPool(t)
	id := deletedCleanupImage(t, p)
	claim := claimCleanup(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var deadline time.Time
	if e := p.QueryRow(ctx, "UPDATE images SET cleanup_lease_until=clock_timestamp()+interval '1 second' RETURNING cleanup_lease_until").Scan(&deadline); e != nil {
		t.Fatal(e)
	}
	locker, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer locker.Rollback(context.Background())
	if _, e := locker.Exec(ctx, "SELECT id FROM images WHERE id=$1 FOR UPDATE", id); e != nil {
		t.Fatal(e)
	}
	cfg := p.Config()
	app := cfg.ConnConfig.RuntimeParams["search_path"] + "_cleanup_wait"
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiter, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { cancel(); locker.Rollback(context.Background()); waiter.Close() }()
	done := make(chan error, 1)
	go func() { done <- cleanupRepo(t, waiter).Complete(ctx, id, claim.Token, deadline.Add(time.Minute)) }()
	awaitOutboxState(t, ctx, func() (bool, error) {
		var waiting bool
		e := p.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND xact_start<$2)", app, deadline).Scan(&waiting)
		return waiting, e
	})
	awaitOutboxState(t, ctx, func() (bool, error) {
		var expired bool
		e := p.QueryRow(ctx, "SELECT clock_timestamp()>=$1", deadline).Scan(&expired)
		return expired, e
	})
	if e := locker.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if !errors.Is(e, repository.ErrCleanupFenced) {
			t.Fatal("lease after wait", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestRecoverIndividualDelays(t *testing.T) {
	p := migratedReadPool(t)
	ctx := context.Background()
	image := uploadInput()
	registerUpload(t, p, image)
	stored, e := uploadRepo(t, p).Create(ctx, image)
	if e != nil {
		t.Fatal(e)
	}
	for i, j := range stored.Jobs {
		claimJob(t, p, j.ID)
		if _, e := p.Exec(ctx, "UPDATE jobs SET attempts=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID, i+1); e != nil {
			t.Fatal(e)
		}
	}
	jobs, e := repository.NewJobRepository(p, repository.JobRepositoryOptions{JobsTopic: "image.processing.jobs", DLQTopic: "image.processing.dlq", MaxAttempts: 3, LeaseDuration: time.Second, RedispatchInterval: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	r := repository.NewRecoveryRepository(jobs)
	before := deleteSnapshot(t, p)
	if _, e := r.Recover(ctx, 3, func(int) time.Duration { return 0 }); !errors.Is(e, repository.ErrInvalidJobInput) {
		t.Fatal("invalid delay accepted", e)
	}
	if before != deleteSnapshot(t, p) {
		t.Fatal("invalid delay left partial recovery")
	}
	calls := map[int]int{}
	ids, e := r.Recover(ctx, 3, func(attempts int) time.Duration { calls[attempts]++; return time.Duration(attempts) * time.Minute })
	if e != nil || len(ids) != 3 {
		t.Fatalf("batch %v %v", ids, e)
	}
	if calls[1] != 1 || calls[2] != 1 || calls[3] != 0 {
		t.Fatalf("retry policy called incorrectly: %v", calls)
	}
	got, e := repository.NewImageRepository(p).GetByID(ctx, image.ID)
	if e != nil {
		t.Fatal(e)
	}
	for _, j := range got.Jobs {
		if j.Attempts == 3 {
			if j.Status != domain.JobStatusFailed || !j.NextRetryAt.IsZero() {
				t.Fatal("exhausted job retried")
			}
			continue
		}
		if j.Status != domain.JobStatusRetrying || !j.NextRetryAt.Equal(j.UpdatedAt.Add(time.Duration(j.Attempts)*time.Minute)) {
			t.Fatalf("individual schedule lost: %+v", j)
		}
		var available time.Time
		if e := p.QueryRow(ctx, "SELECT available_at FROM outbox WHERE job_id=$1 AND published_at IS NULL AND event_type='image.processing.requested'", j.ID).Scan(&available); e != nil || !available.Equal(j.NextRetryAt) {
			t.Fatal("outbox schedule differs", e)
		}
	}
}

// cleanupScanBarrier pauses after the unlocked candidate scan, before rechecking.
type cleanupScanBarrier struct {
	ready, release chan struct{}
	once           sync.Once
}
type cleanupScanKey struct{}

func (b *cleanupScanBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, cleanupScanKey{}, strings.HasPrefix(data.SQL, "SELECT object_key FROM storage_objects WHERE object_key COLLATE"))
}
func (b *cleanupScanBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(cleanupScanKey{}).(bool); match && data.Err == nil {
		b.once.Do(func() {
			close(b.ready)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestCleanupCursorAfterRecheck(t *testing.T) {
	for _, excludeAll := range []bool{false, true} {
		name := "last excluded"
		if excludeAll {
			name = "empty page"
		}
		t.Run(name, func(t *testing.T) {
			p := migratedReadPool(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, key := range []string{"a", "b", "c"} {
				if _, e := repository.NewStorageRepository(p).RegisterOriginal(ctx, key, uuid.NewString(), time.Minute); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 hour'"); e != nil {
				t.Fatal(e)
			}
			barrier := &cleanupScanBarrier{ready: make(chan struct{}), release: make(chan struct{})}
			cfg := p.Config()
			cfg.ConnConfig.Tracer = barrier
			client, e := pgxpool.NewWithConfig(ctx, cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { cancel(); client.Close() }()
			type result struct {
				page repository.CleanupPage
				e    error
			}
			done := make(chan result, 1)
			go func() { page, e := cleanupRepo(t, client).FindDeletable(ctx, 2, "", 0); done <- result{page, e} }()
			select {
			case <-barrier.ready:
			case <-ctx.Done():
				t.Fatal("candidate scan not reached")
			}
			keys := []string{"b"}
			if excludeAll {
				keys = append(keys, "a")
			}
			if _, e := p.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()+interval '1 hour' WHERE object_key=ANY($1::text[])", keys); e != nil {
				t.Fatal(e)
			}
			close(barrier.release)
			var page repository.CleanupPage
			select {
			case got := <-done:
				if got.e != nil {
					t.Fatal(got.e)
				}
				page = got.page
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want := 1
			if excludeAll {
				want = 0
			}
			if len(page.Objects) != want || page.NextCursor != "b" {
				t.Fatalf("scan progress lost: %+v", page)
			}
			next, e := cleanupRepo(t, p).FindDeletable(ctx, 2, page.NextCursor, 0)
			if e != nil || len(next.Objects) != 1 || next.Objects[0].ObjectKey != "c" || next.NextCursor != "c" {
				t.Fatalf("following candidate skipped: %+v %v", next, e)
			}
			end, e := cleanupRepo(t, p).FindDeletable(ctx, 2, next.NextCursor, 0)
			if e != nil || len(end.Objects) != 0 || end.NextCursor != "" {
				t.Fatalf("end cursor %+v %v", end, e)
			}
		})
	}
}
