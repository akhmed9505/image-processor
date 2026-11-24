//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func jobRepo(t *testing.T, pool *pgxpool.Pool) *repository.JobRepository {
	t.Helper()
	repo, err := repository.NewJobRepository(pool, repository.JobRepositoryOptions{JobsTopic: "image.processing.jobs", DLQTopic: "image.processing.dlq", MaxAttempts: 2, LeaseDuration: 30 * time.Second, RedispatchInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}
func seedJob(t *testing.T, pool *pgxpool.Pool) domain.Job {
	t.Helper()
	image := uploadInput()
	image.Jobs = image.Jobs[:1]
	registerUpload(t, pool, image)
	stored, err := uploadRepo(t, pool).Create(context.Background(), image)
	if err != nil {
		t.Fatal(err)
	}
	return stored.Jobs[0]
}
func claimJob(t *testing.T, pool *pgxpool.Pool, id string) repository.ClaimResult {
	t.Helper()
	result, err := jobRepo(t, pool).Claim(context.Background(), id)
	if err != nil || result.Disposition != repository.ClaimAcquired || !result.CanAck() {
		t.Fatalf("claim: %+v, %v", result, err)
	}
	return result
}
func storedJob(t *testing.T, pool *pgxpool.Pool) domain.Job {
	t.Helper()
	image, err := repository.NewImageRepository(pool).GetByID(context.Background(), readImageID(1))
	if err != nil {
		t.Fatal(err)
	}
	return image.Jobs[0]
}
func retryTime(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(context.Background(), "SELECT clock_timestamp()+interval '1 minute'").Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}
func resultFor(claim repository.ClaimResult) repository.JobResult {
	return repository.JobResult{ObjectKey: claim.ResultKey, Width: 600, Height: 400, SizeBytes: 1000}
}

var transientJobFailure = repository.JobFailure{Code: "storage_unavailable", Message: "storage temporarily unavailable"}

func TestJobClaimComplete(t *testing.T) {
	pool := migratedReadPool(t)
	initial := seedJob(t, pool)
	claim := claimJob(t, pool, initial.ID)
	repo := jobRepo(t, pool)
	ctx := context.Background()
	if claim.Job.Status != domain.JobStatusProcessing || claim.Job.Attempts != 1 || claim.Job.AttemptToken == "" || !claim.Job.NextRetryAt.IsZero() {
		t.Fatalf("incorrect attempt: %+v", claim)
	}
	if parsed, err := uuid.Parse(claim.Job.AttemptToken); err != nil || parsed.Version() != 4 {
		t.Fatal("attempt token is not UUID v4")
	}
	if claim.ResultKey != claim.Job.ImageID+"/results/"+claim.Job.ID+"/"+claim.Job.AttemptToken+".png" {
		t.Fatal("incorrect immutable result key")
	}
	intent, err := repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
	if err != nil || intent.JobID != initial.ID || intent.ImageID != initial.ImageID || intent.AttemptToken != claim.Job.AttemptToken || !intent.WriteUntil.Equal(claim.Job.LeaseUntil) || intent.State != repository.StorageStateWriting {
		t.Fatalf("result intent: %+v, %v", intent, err)
	}
	duplicate, err := repo.Claim(ctx, initial.ID)
	if err != nil || duplicate.Disposition != repository.ClaimProcessing || !duplicate.CanAck() || duplicate.ResultKey != "" || duplicate.Job.ID != "" {
		t.Fatalf("duplicate: %+v, %v", duplicate, err)
	}
	done, err := repo.Complete(ctx, initial.ID, claim.Job.AttemptToken, resultFor(claim))
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != domain.JobStatusCompleted || done.ResultKey != claim.ResultKey || done.ResultWidth != 600 || done.ResultHeight != 400 || done.ResultSizeBytes != 1000 || done.AttemptToken != "" || !done.LeaseUntil.IsZero() || done.CompletedAt.IsZero() {
		t.Fatalf("completion: %+v", done)
	}
	intent, err = repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
	if err != nil || intent.State != repository.StorageStateRetained {
		t.Fatal("completed intent not retained")
	}
	terminal, err := repo.Claim(ctx, initial.ID)
	if err != nil || terminal.Disposition != repository.ClaimTerminal || !terminal.CanAck() {
		t.Fatalf("terminal duplicate: %+v %v", terminal, err)
	}
	if _, err := repo.Complete(ctx, initial.ID, claim.Job.AttemptToken, resultFor(claim)); !errors.Is(err, repository.ErrAttemptFenced) {
		t.Fatalf("repeated completion: %v", err)
	}
	if got := storedJob(t, pool); !reflect.DeepEqual(got, done) {
		t.Fatal("stored completion differs")
	}
}

func TestJobRetryAndExhaustion(t *testing.T) {
	pool := migratedReadPool(t)
	initial := seedJob(t, pool)
	claim := claimJob(t, pool, initial.ID)
	repo := jobRepo(t, pool)
	ctx := context.Background()
	at := retryTime(t, pool)
	// Simulate an unmarked publish: the delayed request must survive its old publisher.
	if _, err := pool.Exec(ctx, "UPDATE outbox SET claim_token=$1,lease_until=clock_timestamp()+interval '30 seconds'", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	retried, err := repo.Retry(ctx, initial.ID, claim.Job.AttemptToken, transientJobFailure, at)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != domain.JobStatusRetrying || !retried.NextRetryAt.Equal(at) || retried.LastErrorCode != transientJobFailure.Code || retried.LastError != transientJobFailure.Message || retried.AttemptToken != "" {
		t.Fatalf("retry: %+v", retried)
	}
	var count int
	var available, dispatch time.Time
	var token, lease *string
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM outbox),o.available_at,j.next_dispatch_at,o.claim_token::text,o.lease_until::text
 FROM outbox o JOIN jobs j ON j.id=o.job_id WHERE j.id=$1`, initial.ID).Scan(&count, &available, &dispatch, &token, &lease); err != nil {
		t.Fatal(err)
	}
	if count != 1 || !available.Equal(at) || !dispatch.Equal(at.Add(time.Minute)) || token != nil || lease != nil {
		t.Fatal("retry notification was duplicated or not durably rescheduled")
	}
	intent, err := repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
	if err != nil || intent.State != repository.StorageStateDiscarded {
		t.Fatal("failed attempt intent not discarded")
	}
	waiting, err := repo.Claim(ctx, initial.ID)
	if err != nil || waiting.Disposition != repository.ClaimWaitingRetry || !waiting.CanAck() {
		t.Fatalf("early retry: %+v, %v", waiting, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET next_retry_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	second := claimJob(t, pool, initial.ID)
	if second.Job.Attempts != 2 || second.Job.AttemptToken == claim.Job.AttemptToken || second.ResultKey == claim.ResultKey {
		t.Fatal("attempt identity reused")
	}
	if _, err := repo.Fail(ctx, initial.ID, claim.Job.AttemptToken, transientJobFailure); !errors.Is(err, repository.ErrAttemptFenced) {
		t.Fatalf("stale token: %v", err)
	}
	failed, err := repo.Retry(ctx, initial.ID, second.Job.AttemptToken, transientJobFailure, retryTime(t, pool))
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != domain.JobStatusFailed || failed.Attempts != 2 || failed.CompletedAt.IsZero() {
		t.Fatalf("exhaustion: %+v", failed)
	}
	assertJobDLQ(t, pool, failed)
	intent, err = repository.NewStorageRepository(pool).GetByKey(ctx, second.ResultKey)
	if err != nil || intent.State != repository.StorageStateDiscarded {
		t.Fatal("last failed intent not discarded")
	}
}

func assertJobDLQ(t *testing.T, pool *pgxpool.Pool, job domain.Job) {
	t.Helper()
	var payloadJSON []byte
	var key, topic string
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT payload,message_key,topic,(SELECT count(*) FROM outbox WHERE event_type='image.processing.failed') FROM outbox WHERE event_type='image.processing.failed'").Scan(&payloadJSON, &key, &topic, &count); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatal(err)
	}
	if count != 1 || key != job.ID || topic != "image.processing.dlq" || payload["job_id"] != job.ID || payload["image_id"] != job.ImageID || payload["attempts"] != float64(job.Attempts) || payload["version"] != float64(1) || payload["event_type"] != "image.processing.failed" || payload["error_code"] != job.LastErrorCode || payload["failed_at"] == nil {
		t.Fatalf("invalid DLQ payload: %s", payloadJSON)
	}
}

func TestJobFinalFailureAndClaimLimit(t *testing.T) {
	for _, fromClaim := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent failure", true: "claim limit"}[fromClaim], func(t *testing.T) {
			pool := migratedReadPool(t)
			initial := seedJob(t, pool)
			repo := jobRepo(t, pool)
			ctx := context.Background()
			var failed domain.Job
			if fromClaim {
				if _, err := pool.Exec(ctx, "UPDATE jobs SET attempts=2"); err != nil {
					t.Fatal(err)
				}
				result, err := repo.Claim(ctx, initial.ID)
				if err != nil || result.Disposition != repository.ClaimExhausted || !result.CanAck() {
					t.Fatalf("exhausted claim: %+v, %v", result, err)
				}
				failed = storedJob(t, pool)
			} else {
				attempt := claimJob(t, pool, initial.ID)
				var err error
				failed, err = repo.Fail(ctx, initial.ID, attempt.Job.AttemptToken, repository.JobFailure{Code: "invalid_original", Message: "original cannot be decoded"})
				if err != nil {
					t.Fatal(err)
				}
			}
			if failed.Status != domain.JobStatusFailed {
				t.Fatal("job not failed")
			}
			assertJobDLQ(t, pool, failed)
			if _, err := repo.Claim(ctx, initial.ID); err != nil {
				t.Fatal(err)
			}
			assertJobDLQ(t, pool, failed)
		})
	}
}

func TestJobAttemptFencing(t *testing.T) {
	for _, kind := range []string{"wrong token", "expired lease", "expired intent", "discarded intent", "deleted image", "wrong result key"} {
		t.Run(kind, func(t *testing.T) {
			pool := migratedReadPool(t)
			initial := seedJob(t, pool)
			claim := claimJob(t, pool, initial.ID)
			repo := jobRepo(t, pool)
			ctx := context.Background()
			token := claim.Job.AttemptToken
			switch kind {
			case "wrong token":
				token = uuid.NewString()
			case "expired lease":
				if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second'"); err != nil {
					t.Fatal(err)
				}
			case "expired intent":
				if _, err := pool.Exec(ctx, "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 second' WHERE job_id IS NOT NULL"); err != nil {
					t.Fatal(err)
				}
			case "discarded intent":
				if _, err := pool.Exec(ctx, "UPDATE storage_objects SET state='discarded' WHERE job_id IS NOT NULL"); err != nil {
					t.Fatal(err)
				}
			case "deleted image":
				if _, err := pool.Exec(ctx, "UPDATE images SET deleted_at=clock_timestamp()"); err != nil {
					t.Fatal(err)
				}
			}
			before, err := repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
			if err != nil {
				t.Fatal(err)
			}
			result := resultFor(claim)
			if kind == "wrong result key" {
				result.ObjectKey = "another-attempt"
			}
			if _, err := repo.Complete(ctx, initial.ID, token, result); !errors.Is(err, repository.ErrAttemptFenced) {
				t.Fatalf("complete was not fenced: %v", err)
			}
			if kind != "wrong result key" {
				if _, err := repo.Retry(ctx, initial.ID, token, transientJobFailure, retryTime(t, pool)); !errors.Is(err, repository.ErrAttemptFenced) {
					t.Fatalf("retry was not fenced: %v", err)
				}
				if _, err := repo.Fail(ctx, initial.ID, token, transientJobFailure); !errors.Is(err, repository.ErrAttemptFenced) {
					t.Fatalf("fail was not fenced: %v", err)
				}
			}
			after, err := repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("fenced attempt modified intent")
			}
			var state string
			if err := pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", initial.ID).Scan(&state); err != nil || state != "processing" {
				t.Fatal("fenced attempt modified job")
			}
			if kind == "deleted image" {
				result, err := repo.Claim(ctx, initial.ID)
				if err != nil || result.Disposition != repository.ClaimImageDeleted || !result.CanAck() {
					t.Fatalf("deleted claim: %+v %v", result, err)
				}
			}
			if kind == "expired lease" {
				result, err := repo.Claim(ctx, initial.ID)
				if !errors.Is(err, repository.ErrJobNeedsRecovery) || result.CanAck() {
					t.Fatalf("expired claim must await recovery: %+v %v", result, err)
				}
			}
		})
	}
}

func TestJobConcurrentClaim(t *testing.T) {
	pool := migratedReadPool(t)
	initial := seedJob(t, pool)
	repo := jobRepo(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	type outcome struct {
		result repository.ClaimResult
		err    error
	}
	done := make(chan outcome, 8)
	for i := 0; i < 8; i++ {
		go func() { <-start; result, err := repo.Claim(ctx, initial.ID); done <- outcome{result, err} }()
	}
	close(start)
	acquired := 0
	for i := 0; i < 8; i++ {
		select {
		case got := <-done:
			if got.err != nil || !got.result.CanAck() {
				t.Fatalf("claim: %+v %v", got.result, got.err)
			}
			if got.result.Disposition == repository.ClaimAcquired {
				acquired++
			} else if got.result.Disposition != repository.ClaimProcessing {
				t.Fatalf("unexpected disposition %s", got.result.Disposition)
			}
		case <-ctx.Done():
			t.Fatal("claim race timed out")
		}
	}
	var intents int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM storage_objects WHERE job_id=$1", initial.ID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if acquired != 1 || intents != 1 || storedJob(t, pool).Attempts != 1 {
		t.Fatal("concurrent claim created extra attempts or intents")
	}
	result, err := repo.Claim(ctx, readImageID(99))
	if !errors.Is(err, repository.ErrJobNotFound) || result.CanAck() {
		t.Fatalf("missing job: %+v %v", result, err)
	}
}

func TestJobRollback(t *testing.T) {
	for _, operation := range []string{"claim", "complete", "retry", "fail"} {
		t.Run(operation, func(t *testing.T) {
			pool := migratedReadPool(t)
			initial := seedJob(t, pool)
			repo := jobRepo(t, pool)
			ctx := context.Background()
			var claim repository.ClaimResult
			if operation != "claim" {
				claim = claimJob(t, pool, initial.ID)
			}
			before := storedJob(t, pool)
			table, event := "storage_objects", "INSERT"
			if operation == "complete" {
				event = "UPDATE"
			}
			if operation == "retry" || operation == "fail" {
				table = "outbox"
				event = "INSERT OR UPDATE"
			}
			if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_job_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE='P0001'; END $$;
   CREATE TRIGGER reject_job_write BEFORE `+event+` ON `+table+` FOR EACH ROW EXECUTE FUNCTION reject_job_write()`); err != nil {
				t.Fatal(err)
			}
			var err error
			switch operation {
			case "claim":
				var result repository.ClaimResult
				result, err = repo.Claim(ctx, initial.ID)
				if result.CanAck() {
					t.Fatal("failed handoff is ackable")
				}
			case "complete":
				_, err = repo.Complete(ctx, initial.ID, claim.Job.AttemptToken, resultFor(claim))
			case "retry":
				_, err = repo.Retry(ctx, initial.ID, claim.Job.AttemptToken, transientJobFailure, retryTime(t, pool))
			case "fail":
				_, err = repo.Fail(ctx, initial.ID, claim.Job.AttemptToken, transientJobFailure)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
				t.Fatalf("expected original database error, got %v", err)
			}
			if after := storedJob(t, pool); !reflect.DeepEqual(before, after) {
				t.Fatal("rollback changed job")
			}
			var intents int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM storage_objects WHERE job_id=$1", initial.ID).Scan(&intents); err != nil {
				t.Fatal(err)
			}
			if operation == "claim" && intents != 0 {
				t.Fatal("claim rollback left an intent")
			}
			if operation != "claim" {
				intent, err := repository.NewStorageRepository(pool).GetByKey(ctx, claim.ResultKey)
				if err != nil || intent.State != repository.StorageStateWriting {
					t.Fatal("outcome rollback changed intent")
				}
			}
			assertUploadCounts(t, pool, 1, 1, 1)
		})
	}
}

func TestJobLeaseExpiresWaitingForStorageLock(t *testing.T) {
	pool := migratedReadPool(t)
	initial := seedJob(t, pool)
	claim := claimJob(t, pool, initial.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var deadline time.Time
	if err := pool.QueryRow(ctx, "UPDATE jobs SET lease_until=clock_timestamp()+interval '1 second' WHERE id=$1 RETURNING lease_until", initial.ID).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(ctx, "SELECT object_key FROM storage_objects WHERE object_key=$1 FOR UPDATE", claim.ResultKey); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	application := cfg.ConnConfig.RuntimeParams["search_path"] + "_job_wait"
	cfg.ConnConfig.RuntimeParams["application_name"] = application
	waiter, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); locker.Rollback(context.Background()); waiter.Close() }()
	repo := jobRepo(t, waiter)
	done := make(chan error, 1)
	go func() {
		_, err := repo.Complete(ctx, initial.ID, claim.Job.AttemptToken, resultFor(claim))
		done <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var started time.Time
	for {
		err := pool.QueryRow(ctx, "SELECT xact_start FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'", application).Scan(&started)
		if err == nil {
			break
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("attempt did not wait for storage lock")
		}
	}
	if !started.Before(deadline) {
		t.Fatal("lease expired before the test attempt started")
	}
	for {
		var expired bool
		if err := pool.QueryRow(ctx, "SELECT clock_timestamp()>=$1", deadline).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("lease did not expire")
		}
	}
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrAttemptFenced) {
			t.Fatalf("lease expiry after storage lock: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("attempt did not finish")
	}
	if storedJob(t, pool).Status != domain.JobStatusProcessing {
		t.Fatal("expired attempt changed job")
	}
}

func TestJobUncertainCommit(t *testing.T) {
	for _, operation := range []string{"claim", "complete"} {
		t.Run(operation, func(t *testing.T) {
			pool := migratedReadPool(t)
			initial := seedJob(t, pool)
			var claim repository.ClaimResult
			if operation == "complete" {
				claim = claimJob(t, pool, initial.ID)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			blocked := make(chan struct{})
			release := make(chan struct{})
			cfg := pool.Config()
			dial := cfg.ConnConfig.DialFunc
			cfg.ConnConfig.DialFunc = func(ctxDial context.Context, network, address string) (net.Conn, error) {
				conn, err := dial(ctxDial, network, address)
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
			repo := jobRepo(t, client)
			done := make(chan error, 1)
			ack := make(chan bool, 1)
			go func() {
				if operation == "claim" {
					result, err := repo.Claim(ctx, initial.ID)
					ack <- result.CanAck()
					done <- err
				} else {
					_, err := repo.Complete(ctx, initial.ID, claim.Job.AttemptToken, resultFor(claim))
					done <- err
				}
			}()
			select {
			case <-blocked:
			case err := <-done:
				t.Fatalf("COMMIT was not intercepted: %v", err)
			case <-ctx.Done():
				t.Fatal("COMMIT not reached")
			}
			desired := "processing"
			if operation == "complete" {
				desired = "completed"
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				var status string
				if err := pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", initial.ID).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if status == desired {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("server did not commit attempt")
				}
			}
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, repository.ErrJobCommitUncertain) || !errors.Is(err, pgconn.ErrConnClosed) {
					t.Fatalf("uncertain commit lost cause: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("commit error not returned")
			}
			if operation == "claim" {
				if <-ack {
					t.Fatal("uncertain claim permitted acknowledgment")
				}
			}
			duplicate, err := jobRepo(t, pool).Claim(ctx, initial.ID)
			if err != nil || !duplicate.CanAck() {
				t.Fatalf("redelivery after uncertain commit: %+v %v", duplicate, err)
			}
		})
	}
}
