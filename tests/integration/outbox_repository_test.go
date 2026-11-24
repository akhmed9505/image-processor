//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func outboxRepo(t *testing.T, pool *pgxpool.Pool) *repository.OutboxRepository {
	t.Helper()
	repo, err := repository.NewOutboxRepository(pool, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func seedOutbox(t *testing.T, pool *pgxpool.Pool, count int) []string {
	t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = uuid.NewString()
		if _, err := pool.Exec(context.Background(), `INSERT INTO outbox(id,event_type,payload,topic,message_key,available_at)
 VALUES ($1,'test.event','{"version":1}','test.topic','test.key',clock_timestamp()-interval '1 minute')`, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func claimOutbox(t *testing.T, pool *pgxpool.Pool) repository.OutboxEvent {
	t.Helper()
	events, err := outboxRepo(t, pool).Claim(context.Background(), 1)
	if err != nil || len(events) != 1 {
		t.Fatalf("claim one event: %+v, %v", events, err)
	}
	return events[0]
}

func outboxSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(context.Background(), "SELECT jsonb_agg(to_jsonb(o) ORDER BY id)::text FROM outbox o").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestOutboxClaimEligibility(t *testing.T) {
	pool := migratedReadPool(t)
	ids := seedOutbox(t, pool, 6)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, item := range []struct{ id, change string }{
		{ids[0], "published_at=clock_timestamp()"},
		{ids[1], "available_at=clock_timestamp()+interval '1 hour'"},
		{ids[2], "claim_token=gen_random_uuid(),lease_until=clock_timestamp()+interval '1 hour'"},
	} {
		if _, err := pool.Exec(ctx, "UPDATE outbox SET "+item.change+" WHERE id=$1", item.id); err != nil {
			t.Fatal(err)
		}
	}
	// A locked eligible row must be skipped rather than blocking the publisher.
	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(ctx, "SELECT id FROM outbox WHERE id=$1 FOR UPDATE", ids[3]); err != nil {
		t.Fatal(err)
	}
	repo := outboxRepo(t, pool)
	events, err := repo.Claim(ctx, 1)
	if err != nil || len(events) != 1 || events[0].ID != ids[4] {
		t.Fatalf("limited claim skipped wrong rows: %+v, %v", events, err)
	}
	event := events[0]
	if event.Topic != "test.topic" || event.MessageKey != "test.key" || event.EventType != "test.event" || event.PublishAttempts != 1 || event.LeaseUntil.IsZero() {
		t.Fatalf("incorrect claimed metadata: %+v", event)
	}
	if token, err := uuid.Parse(event.ClaimToken); err != nil || token.Version() != 4 {
		t.Fatal("claim token is not UUID v4")
	}
	var payload map[string]int
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload["version"] != 1 {
		t.Fatalf("invalid event payload: %s, %v", event.Payload, err)
	}
	events, err = repo.Claim(ctx, 10)
	if err != nil || len(events) != 1 || events[0].ID != ids[5] || events[0].ClaimToken == event.ClaimToken {
		t.Fatalf("second claim: %+v, %v", events, err)
	}
	events, err = repo.Claim(ctx, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("ineligible events claimed: %+v, %v", events, err)
	}
}

func TestOutboxConcurrentClaims(t *testing.T) {
	pool := migratedReadPool(t)
	seedOutbox(t, pool, 24)
	repo := outboxRepo(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type outcome struct {
		events []repository.OutboxEvent
		err    error
	}
	done := make(chan outcome, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() { <-start; events, err := repo.Claim(ctx, 3); done <- outcome{events, err} }()
	}
	close(start)
	seen := make(map[string]bool)
	tokens := make(map[string]bool)
	for i := 0; i < 8; i++ {
		select {
		case result := <-done:
			if result.err != nil || len(result.events) != 3 {
				t.Fatalf("concurrent batch: %+v, %v", result.events, result.err)
			}
			for _, event := range result.events {
				if seen[event.ID] || tokens[event.ClaimToken] || event.PublishAttempts != 1 {
					t.Fatal("overlapping batches or reused claim token")
				}
				seen[event.ID], tokens[event.ClaimToken] = true, true
			}
		case <-ctx.Done():
			t.Fatal("claims did not finish")
		}
	}
	if len(seen) != 24 {
		t.Fatal("events omitted")
	}
}

func TestOutboxPublicationAndReschedule(t *testing.T) {
	pool := migratedReadPool(t)
	seedOutbox(t, pool, 1)
	first := claimOutbox(t, pool)
	repo := outboxRepo(t, pool)
	ctx := context.Background()
	at := retryTime(t, pool)
	if err := repo.Reschedule(ctx, first.ID, first.ClaimToken, "kafka_unavailable", at); err != nil {
		t.Fatal(err)
	}
	var available time.Time
	var code string
	var released bool
	if err := pool.QueryRow(ctx, `SELECT available_at,last_error_code,claim_token IS NULL AND lease_until IS NULL AND published_at IS NULL
 FROM outbox WHERE id=$1`, first.ID).Scan(&available, &code, &released); err != nil || !available.Equal(at) || code != "kafka_unavailable" || !released {
		t.Fatalf("incorrect reschedule: %v %s %v, %v", available, code, released, err)
	}
	events, err := repo.Claim(ctx, 1)
	if err != nil || len(events) != 0 {
		t.Fatal("delayed event was claimed")
	}
	if _, err := pool.Exec(ctx, "UPDATE outbox SET available_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	second := claimOutbox(t, pool)
	if second.ID != first.ID || second.ClaimToken == first.ClaimToken || second.PublishAttempts != 2 || string(second.Payload) != string(first.Payload) {
		t.Fatal("retry lost event identity or claim ownership")
	}
	if err := repo.MarkPublished(ctx, second.ID, second.ClaimToken); err != nil {
		t.Fatal(err)
	}
	var published time.Time
	if err := pool.QueryRow(ctx, `SELECT published_at,claim_token IS NULL AND lease_until IS NULL AND last_error_code IS NULL
 FROM outbox WHERE id=$1`, second.ID).Scan(&published, &released); err != nil || published.IsZero() || !released {
		t.Fatalf("incorrect publication: %v %v, %v", published, released, err)
	}
	if err := repo.MarkPublished(ctx, second.ID, second.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
		t.Fatalf("published event changed twice: %v", err)
	}
}

func TestOutboxFencingAndReclaim(t *testing.T) {
	for _, kind := range []string{"wrong token", "malformed token", "expired lease", "reclaimed", "missing"} {
		t.Run(kind, func(t *testing.T) {
			pool := migratedReadPool(t)
			seedOutbox(t, pool, 1)
			first := claimOutbox(t, pool)
			repo := outboxRepo(t, pool)
			ctx := context.Background()
			id, token := first.ID, first.ClaimToken
			switch kind {
			case "wrong token":
				token = uuid.NewString()
			case "malformed token":
				token = "invalid"
			case "missing":
				id = uuid.NewString()
			case "expired lease", "reclaimed":
				if _, err := pool.Exec(ctx, "UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'"); err != nil {
					t.Fatal(err)
				}
				if kind == "reclaimed" {
					second := claimOutbox(t, pool)
					if second.ID != first.ID || second.ClaimToken == first.ClaimToken || second.PublishAttempts != 2 {
						t.Fatalf("incorrect reclaim: %+v", second)
					}
					if err := repo.MarkPublished(ctx, second.ID, first.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
						t.Fatalf("old token accepted: %v", err)
					}
					if err := repo.Reschedule(ctx, second.ID, first.ClaimToken, "old_error", retryTime(t, pool)); !errors.Is(err, repository.ErrOutboxClaimFenced) {
						t.Fatalf("old token rescheduled a new claim: %v", err)
					}
					if err := repo.MarkPublished(ctx, second.ID, second.ClaimToken); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := outboxSnapshot(t, pool)
			if err := repo.MarkPublished(ctx, id, token); !errors.Is(err, repository.ErrOutboxClaimFenced) {
				t.Fatalf("publication was not fenced: %v", err)
			}
			if err := repo.Reschedule(ctx, id, token, "publish_timeout", retryTime(t, pool)); !errors.Is(err, repository.ErrOutboxClaimFenced) {
				t.Fatalf("reschedule was not fenced: %v", err)
			}
			if after := outboxSnapshot(t, pool); before != after {
				t.Fatal("fenced publisher modified event")
			}
		})
	}
}

func TestOutboxClaimInvalidatedByJobRetry(t *testing.T) {
	pool := migratedReadPool(t)
	job := seedJob(t, pool)
	event := claimOutbox(t, pool)
	attempt := claimJob(t, pool, job.ID)
	ctx := context.Background()
	at := retryTime(t, pool)
	if _, err := jobRepo(t, pool).Retry(ctx, job.ID, attempt.Job.AttemptToken, transientJobFailure, at); err != nil {
		t.Fatal(err)
	}
	before := outboxSnapshot(t, pool)
	repo := outboxRepo(t, pool)
	if err := repo.MarkPublished(ctx, event.ID, event.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
		t.Fatalf("invalidated publication: %v", err)
	}
	if err := repo.Reschedule(ctx, event.ID, event.ClaimToken, "old_error", at.Add(time.Minute)); !errors.Is(err, repository.ErrOutboxClaimFenced) {
		t.Fatalf("invalidated reschedule: %v", err)
	}
	if before != outboxSnapshot(t, pool) {
		t.Fatal("old publisher changed rescheduled request")
	}
	var available time.Time
	var count int
	if err := pool.QueryRow(ctx, "SELECT available_at,(SELECT count(*) FROM outbox) FROM outbox WHERE id=$1", event.ID).Scan(&available, &count); err != nil || !available.Equal(at) || count != 1 {
		t.Fatalf("job request lost or duplicated: %v %d, %v", available, count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE outbox SET available_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	current := claimOutbox(t, pool)
	if current.ID != event.ID || current.ClaimToken == event.ClaimToken || current.PublishAttempts != 2 || string(current.Payload) != string(event.Payload) {
		t.Fatal("rescheduled request changed identity or reused the invalidated claim")
	}
	if err := repo.MarkPublished(ctx, event.ID, event.ClaimToken); !errors.Is(err, repository.ErrOutboxClaimFenced) {
		t.Fatalf("invalidated token overwrote a new claim: %v", err)
	}
	if err := repo.MarkPublished(ctx, current.ID, current.ClaimToken); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxRollback(t *testing.T) {
	for _, operation := range []string{"claim", "publish", "reschedule"} {
		t.Run(operation, func(t *testing.T) {
			pool := migratedReadPool(t)
			seedOutbox(t, pool, 3)
			var event repository.OutboxEvent
			if operation != "claim" {
				event = claimOutbox(t, pool)
			}
			ctx := context.Background()
			// An AFTER STATEMENT failure rolls back the entire batch after row updates.
			if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_outbox_update() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$;
 CREATE TRIGGER reject_outbox_update AFTER UPDATE ON outbox FOR EACH STATEMENT EXECUTE FUNCTION reject_outbox_update()`); err != nil {
				t.Fatal(err)
			}
			before := outboxSnapshot(t, pool)
			repo := outboxRepo(t, pool)
			var err error
			switch operation {
			case "claim":
				var events []repository.OutboxEvent
				events, err = repo.Claim(ctx, 3)
				if len(events) != 0 {
					t.Fatal("failed claim returned events")
				}
			case "publish":
				err = repo.MarkPublished(ctx, event.ID, event.ClaimToken)
			case "reschedule":
				err = repo.Reschedule(ctx, event.ID, event.ClaimToken, "publish_timeout", retryTime(t, pool))
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || errors.Is(err, repository.ErrOutboxCommitUncertain) {
				t.Fatalf("rollback lost database cause: %v", err)
			}
			if before != outboxSnapshot(t, pool) {
				t.Fatal("rollback changed events")
			}
		})
	}
}

// awaitOutboxState polls a PostgreSQL condition rather than sleeping for a guess.
func awaitOutboxState(t *testing.T, ctx context.Context, ready func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("outbox state not reached: ", ctx.Err())
		}
	}
}

func TestOutboxLeaseExpiresWaitingForLock(t *testing.T) {
	for _, operation := range []string{"publish", "reschedule"} {
		t.Run(operation, func(t *testing.T) {
			pool := migratedReadPool(t)
			seedOutbox(t, pool, 1)
			event := claimOutbox(t, pool)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var deadline time.Time
			if err := pool.QueryRow(ctx, "UPDATE outbox SET lease_until=clock_timestamp()+interval '1 second' RETURNING lease_until").Scan(&deadline); err != nil {
				t.Fatal(err)
			}
			before := outboxSnapshot(t, pool)
			locker, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer locker.Rollback(context.Background())
			if _, err := locker.Exec(ctx, "SELECT id FROM outbox WHERE id=$1 FOR UPDATE", event.ID); err != nil {
				t.Fatal(err)
			}
			cfg := pool.Config()
			application := cfg.ConnConfig.RuntimeParams["search_path"] + "_outbox_wait"
			cfg.ConnConfig.RuntimeParams["application_name"] = application
			waiter, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); locker.Rollback(context.Background()); waiter.Close() }()
			repo := outboxRepo(t, waiter)
			done := make(chan error, 1)
			go func() {
				if operation == "publish" {
					done <- repo.MarkPublished(ctx, event.ID, event.ClaimToken)
				} else {
					done <- repo.Reschedule(ctx, event.ID, event.ClaimToken, "publish_timeout", deadline.Add(time.Minute))
				}
			}()
			var started time.Time
			awaitOutboxState(t, ctx, func() (bool, error) {
				err := pool.QueryRow(ctx, "SELECT xact_start FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'", application).Scan(&started)
				if errors.Is(err, pgx.ErrNoRows) {
					return false, nil
				}
				return err == nil, err
			})
			if !started.Before(deadline) {
				t.Fatal("lease expired before finalization started")
			}
			awaitOutboxState(t, ctx, func() (bool, error) {
				var expired bool
				err := pool.QueryRow(ctx, "SELECT clock_timestamp()>=$1", deadline).Scan(&expired)
				return expired, err
			})
			if err := locker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, repository.ErrOutboxClaimFenced) {
					t.Fatalf("lease expiry after lock was ignored: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("finalization did not finish")
			}
			if before != outboxSnapshot(t, pool) {
				t.Fatal("expired publisher changed event")
			}
		})
	}
}

func TestOutboxUncertainCommit(t *testing.T) {
	for _, operation := range []string{"claim", "publish", "reschedule"} {
		t.Run(operation, func(t *testing.T) {
			pool := migratedReadPool(t)
			ids := seedOutbox(t, pool, 1)
			var event repository.OutboxEvent
			if operation != "claim" {
				event = claimOutbox(t, pool)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			at := retryTime(t, pool)
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
			repo := outboxRepo(t, client)
			type outcome struct {
				events []repository.OutboxEvent
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				var result outcome
				switch operation {
				case "claim":
					result.events, result.err = repo.Claim(ctx, 1)
				case "publish":
					result.err = repo.MarkPublished(ctx, event.ID, event.ClaimToken)
				case "reschedule":
					result.err = repo.Reschedule(ctx, event.ID, event.ClaimToken, "publish_timeout", at)
				}
				done <- result
			}()
			select {
			case <-blocked:
			case result := <-done:
				t.Fatalf("COMMIT was not intercepted: %v", result.err)
			case <-ctx.Done():
				t.Fatal("COMMIT not reached")
			}
			awaitOutboxState(t, ctx, func() (bool, error) {
				var claimed, published bool
				var available time.Time
				err := pool.QueryRow(ctx, "SELECT claim_token IS NOT NULL,published_at IS NOT NULL,available_at FROM outbox WHERE id=$1", ids[0]).Scan(&claimed, &published, &available)
				switch operation {
				case "claim":
					return claimed, err
				case "publish":
					return published, err
				default:
					return !claimed && available.Equal(at), err
				}
			})
			committed := outboxSnapshot(t, pool)
			close(release)
			select {
			case result := <-done:
				if !errors.Is(result.err, repository.ErrOutboxCommitUncertain) || !errors.Is(result.err, pgconn.ErrConnClosed) || len(result.events) != 0 {
					t.Fatalf("uncertain COMMIT lost cause or returned events: %+v, %v", result.events, result.err)
				}
			case <-ctx.Done():
				t.Fatal("COMMIT error not returned")
			}
			if committed != outboxSnapshot(t, pool) {
				t.Fatal("uncertain COMMIT triggered compensation")
			}
			if operation == "claim" {
				if _, err := pool.Exec(ctx, "UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'"); err != nil {
					t.Fatal(err)
				}
				if recovered := claimOutbox(t, pool); recovered.ID != ids[0] || recovered.PublishAttempts != 2 {
					t.Fatal("uncertain claim could not be reclaimed")
				}
			}
		})
	}
}

func TestOutboxInvalidInput(t *testing.T) {
	for _, lease := range []time.Duration{0, -time.Second, time.Nanosecond} {
		if _, err := repository.NewOutboxRepository(nil, lease); !errors.Is(err, repository.ErrInvalidOutboxInput) {
			t.Fatalf("invalid lease accepted: %v", err)
		}
	}
	pool := migratedReadPool(t)
	seedOutbox(t, pool, 1)
	event := claimOutbox(t, pool)
	repo := outboxRepo(t, pool)
	ctx := context.Background()
	before := outboxSnapshot(t, pool)
	for _, limit := range []int{0, -1} {
		if events, err := repo.Claim(ctx, limit); !errors.Is(err, repository.ErrInvalidOutboxInput) || len(events) != 0 {
			t.Fatalf("invalid limit accepted: %v", err)
		}
	}
	for _, item := range []struct {
		code string
		at   time.Time
	}{{"", retryTime(t, pool)}, {"timeout", time.Time{}}, {"timeout", time.Unix(1, 0)}} {
		if err := repo.Reschedule(ctx, event.ID, event.ClaimToken, item.code, item.at); !errors.Is(err, repository.ErrInvalidOutboxInput) {
			t.Fatalf("invalid reschedule accepted: %v", err)
		}
	}
	if before != outboxSnapshot(t, pool) {
		t.Fatal("invalid input modified event")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if events, err := repo.Claim(ctx, 1); !errors.Is(err, context.Canceled) || len(events) != 0 {
		t.Fatalf("claim lost context cause: %v", err)
	}
	if err := repo.MarkPublished(ctx, event.ID, event.ClaimToken); !errors.Is(err, context.Canceled) {
		t.Fatalf("finalization lost context cause: %v", err)
	}
}
