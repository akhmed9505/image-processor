//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func uploadInput() domain.Image {
	return domain.Image{ID: readImageID(1), OriginalName: "photo.png", OriginalKey: "1/original.png", ContentType: "image/png", SizeBytes: 12345, Width: 1200, Height: 800, Jobs: []domain.Job{
		{ID: "10000000-0000-0000-0000-000000000001", Operation: domain.OperationResize, Params: domain.OperationParams{Resize: domain.ResizeParams{Width: 600, Height: 600}}},
		{ID: "10000000-0000-0000-0000-000000000002", Operation: domain.OperationThumbnail, Params: domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 200}}},
		{ID: "10000000-0000-0000-0000-000000000003", Operation: domain.OperationWatermark},
	}}
}

func uploadRepo(t *testing.T, pool *pgxpool.Pool) *repository.UploadRepository {
	t.Helper()
	repo, err := repository.NewUploadRepository(pool, "image.processing.jobs", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func registerUpload(t *testing.T, pool *pgxpool.Pool, image domain.Image) repository.StorageObject {
	t.Helper()
	intent, err := repository.NewStorageRepository(pool).RegisterOriginal(context.Background(), image.OriginalKey, image.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func assertUploadCounts(t *testing.T, pool *pgxpool.Pool, images, jobs, events int) {
	t.Helper()
	var actualImages, actualJobs, actualEvents int
	if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM images),(SELECT count(*) FROM jobs),(SELECT count(*) FROM outbox)`).Scan(&actualImages, &actualJobs, &actualEvents); err != nil {
		t.Fatal(err)
	}
	if actualImages != images || actualJobs != jobs || actualEvents != events {
		t.Fatalf("counts=(%d,%d,%d), want (%d,%d,%d)", actualImages, actualJobs, actualEvents, images, jobs, events)
	}
}

func TestUploadCreate(t *testing.T) {
	pool := migratedReadPool(t)
	input := uploadInput()
	intent := registerUpload(t, pool, input)
	ctx := context.Background()
	image, err := uploadRepo(t, pool).Create(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	assertUploadCounts(t, pool, 1, 3, 3)
	stored, err := repository.NewImageRepository(pool).GetByID(ctx, input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(image, stored) {
		t.Fatalf("returned and persisted image differ: %+v / %+v", image, stored)
	}
	if image.CreatedAt.IsZero() || len(image.Jobs) != 3 {
		t.Fatal("missing generated metadata")
	}
	state, err := image.ProcessingStatus()
	if err != nil || state != domain.ImageStatusPending {
		t.Fatalf("aggregate status=%s error=%v", state, err)
	}
	registry, err := repository.NewStorageRepository(pool).GetByKey(ctx, input.OriginalKey)
	if err != nil || registry.State != repository.StorageStateRetained || !registry.WriteUntil.Equal(intent.WriteUntil) {
		t.Fatalf("intent=%+v error=%v", registry, err)
	}
	for i, job := range image.Jobs {
		if job.Status != domain.JobStatusPending || job.ImageID != image.ID || job.ID != input.Jobs[i].ID || job.Params != input.Jobs[i].Params || job.Attempts != 0 || job.AttemptToken != "" || !job.LeaseUntil.IsZero() {
			t.Fatalf("unexpected pending job: %+v", job)
		}
		var params map[string]int
		var paramsJSON, payloadJSON []byte
		var eventID, topic, key string
		var nextDispatch, available time.Time
		var published *time.Time
		err := pool.QueryRow(ctx, `SELECT j.params,j.next_dispatch_at,o.id::text,o.payload,o.topic,o.message_key,o.available_at,o.published_at
   FROM jobs j JOIN outbox o ON o.job_id=j.id WHERE j.id=$1`, job.ID).Scan(&paramsJSON, &nextDispatch, &eventID, &payloadJSON, &topic, &key, &available, &published)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			t.Fatal(err)
		}
		expected := []map[string]int{{"width": 600, "height": 600}, {"size": 200}, {}}
		if !reflect.DeepEqual(params, expected[i]) {
			t.Fatalf("params=%v, want %v", params, expected[i])
		}
		var payload map[string]any
		if err := json.Unmarshal(payloadJSON, &payload); err != nil {
			t.Fatal(err)
		}
		if len(payload) != 4 || payload["event_id"] != eventID || payload["event_type"] != "image.processing.requested" || payload["version"] != float64(1) || payload["job_id"] != job.ID || key != job.ID || topic != "image.processing.jobs" || published != nil {
			t.Fatalf("incorrect event: %s", payloadJSON)
		}
		if parsed, err := uuid.Parse(eventID); err != nil || parsed.Version() != 4 {
			t.Fatalf("invalid event UUID: %s", eventID)
		}
		if nextDispatch.Before(job.CreatedAt.Add(time.Minute)) || !available.Equal(image.CreatedAt) {
			t.Fatal("incorrect initial event/redispatch schedule")
		}
	}
}

func TestUploadRollback(t *testing.T) {
	for _, failure := range []string{"second job", "second operation", "second outbox", "expiry during writes"} {
		t.Run(failure, func(t *testing.T) {
			pool := migratedReadPool(t)
			image := uploadInput()
			before := registerUpload(t, pool, image)
			var wantCode string
			if failure == "second job" {
				image.Jobs[1].ID = image.Jobs[0].ID
				wantCode = "23505"
			} else if failure == "second operation" {
				image.Jobs[1].Operation = image.Jobs[0].Operation
				image.Jobs[1].Params = image.Jobs[0].Params
				wantCode = "23505"
			} else {
				body := `IF EXISTS (SELECT 1 FROM outbox) THEN RAISE EXCEPTION 'injected outbox failure' USING ERRCODE='P0001'; END IF;`
				wantCode = "P0001"
				if failure == "expiry during writes" {
					body = `UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 second';`
					wantCode = ""
				}
				_, err := pool.Exec(context.Background(), `CREATE FUNCTION fail_upload() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN `+body+` RETURN NEW; END $$;
    CREATE TRIGGER fail_upload BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_upload()`)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err := uploadRepo(t, pool).Create(context.Background(), image)
			if wantCode != "" {
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != wantCode {
					t.Fatalf("expected wrapped SQLSTATE %s, got %v", wantCode, err)
				}
			} else if !errors.Is(err, repository.ErrWriteIntentExpired) {
				t.Fatalf("expected expiration, got %v", err)
			}
			assertUploadCounts(t, pool, 0, 0, 0)
			after, readErr := repository.NewStorageRepository(pool).GetByKey(context.Background(), image.OriginalKey)
			if readErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rollback changed original intent: %+v, %v", after, readErr)
			}
		})
	}
}

func TestUploadRejectedIntents(t *testing.T) {
	for _, kind := range []string{"missing", "wrong key", "foreign image", "expired", "discarded", "retained"} {
		t.Run(kind, func(t *testing.T) {
			pool := migratedReadPool(t)
			image := uploadInput()
			var want error
			switch kind {
			case "missing":
				want = repository.ErrStorageObjectNotFound
			case "wrong key":
				other := image
				other.OriginalKey = "other-original"
				registerUpload(t, pool, other)
				want = repository.ErrStorageObjectNotFound
			case "foreign image":
				other := image
				other.ID = readImageID(2)
				registerUpload(t, pool, other)
				want = repository.ErrInvalidWriteIntent
			default:
				registerUpload(t, pool, image)
				if kind == "expired" {
					if _, err := pool.Exec(context.Background(), "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 second'"); err != nil {
						t.Fatal(err)
					}
					want = repository.ErrWriteIntentExpired
				} else {
					if _, err := pool.Exec(context.Background(), "UPDATE storage_objects SET state=$1", kind); err != nil {
						t.Fatal(err)
					}
					want = repository.ErrInvalidStorageTransition
				}
			}
			if _, err := uploadRepo(t, pool).Create(context.Background(), image); !errors.Is(err, want) {
				t.Fatalf("expected %v, got %v", want, err)
			}
			assertUploadCounts(t, pool, 0, 0, 0)
		})
	}
}

func TestUploadConcurrentFinalization(t *testing.T) {
	pool := migratedReadPool(t)
	image := uploadInput()
	registerUpload(t, pool, image)
	repo := uploadRepo(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { <-start; _, err := repo.Create(ctx, image); done <- err }()
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
			t.Fatal("concurrent finalization timed out")
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful finalizations", successes)
	}
	assertUploadCounts(t, pool, 1, 3, 3)
}

func TestUploadConcurrentDiscard(t *testing.T) {
	pool := migratedReadPool(t)
	image := uploadInput()
	registerUpload(t, pool, image)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	repo := uploadRepo(t, pool)
	start := make(chan struct{})
	created := make(chan error, 1)
	discarded := make(chan error, 1)
	go func() { <-start; _, err := repo.Create(ctx, image); created <- err }()
	go func() {
		<-start
		_, err := repository.NewStorageRepository(pool).DiscardOriginal(ctx, image.OriginalKey)
		discarded <- err
	}()
	close(start)
	var createErr, discardErr error
	select {
	case createErr = <-created:
	case <-ctx.Done():
		t.Fatal("upload timed out")
	}
	select {
	case discardErr = <-discarded:
	case <-ctx.Done():
		t.Fatal("discard timed out")
	}
	stored, err := repository.NewStorageRepository(pool).GetByKey(ctx, image.OriginalKey)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case createErr == nil && errors.Is(discardErr, repository.ErrInvalidStorageTransition):
		if stored.State != repository.StorageStateRetained {
			t.Fatal("accepted upload not retained")
		}
		assertUploadCounts(t, pool, 1, 3, 3)
	case discardErr == nil && errors.Is(createErr, repository.ErrInvalidStorageTransition):
		if stored.State != repository.StorageStateDiscarded {
			t.Fatal("discard was overwritten")
		}
		assertUploadCounts(t, pool, 0, 0, 0)
	default:
		t.Fatalf("invalid race outcome: create=%v discard=%v", createErr, discardErr)
	}
}

func TestUploadExpiryAfterLock(t *testing.T) {
	pool := migratedReadPool(t)
	image := uploadInput()
	registerUpload(t, pool, image)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(ctx, "SELECT object_key FROM storage_objects FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	application := cfg.ConnConfig.RuntimeParams["search_path"] + "_upload_wait"
	cfg.ConnConfig.RuntimeParams["application_name"] = application
	waiter, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); locker.Rollback(context.Background()); waiter.Close() }()
	done := make(chan error, 1)
	repo := uploadRepo(t, waiter)
	go func() { _, err := repo.Create(ctx, image); done <- err }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var started time.Time
	for {
		err := pool.QueryRow(ctx, `SELECT xact_start FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'`, application).Scan(&started)
		if err == nil {
			break
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("upload did not wait for intent lock")
		}
	}
	deadline := started.Add(time.Microsecond)
	var expired bool
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()>$1", deadline).Scan(&expired); err != nil || !expired {
		t.Fatalf("expected elapsed deadline: %v", err)
	}
	if _, err := locker.Exec(ctx, "UPDATE storage_objects SET write_until=$1", deadline); err != nil {
		t.Fatal(err)
	}
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrWriteIntentExpired) {
			t.Fatalf("expiry after lock: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("upload did not finish")
	}
	assertUploadCounts(t, pool, 0, 0, 0)
}

// Drop only the COMMIT response after PostgreSQL has accepted the transaction.
type uploadCommitResponseDrop struct {
	net.Conn
	sent    atomic.Bool
	blocked chan struct{}
	release <-chan struct{}
	ctx     context.Context
}

func (c *uploadCommitResponseDrop) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && bytes.Contains(p, []byte("commit\x00")) {
		c.sent.Store(true)
	}
	return n, err
}
func (c *uploadCommitResponseDrop) Read(p []byte) (int, error) {
	if c.sent.CompareAndSwap(true, false) {
		close(c.blocked)
		select {
		case <-c.release:
		case <-c.ctx.Done():
		}
		c.Conn.Close()
		return 0, io.ErrUnexpectedEOF
	}
	return c.Conn.Read(p)
}

func TestUploadUncertainCommit(t *testing.T) {
	pool := migratedReadPool(t)
	image := uploadInput()
	registerUpload(t, pool, image)
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
	repo := uploadRepo(t, client)
	done := make(chan error, 1)
	go func() { _, err := repo.Create(ctx, image); done <- err }()
	select {
	case <-blocked:
	case err := <-done:
		t.Fatalf("upload ended before COMMIT response interception: %v", err)
	case <-ctx.Done():
		t.Fatal("COMMIT not intercepted")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM images WHERE id=$1)", image.ID).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("server did not commit upload")
		}
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrUploadCommitUncertain) || !errors.Is(err, pgconn.ErrConnClosed) {
			t.Fatalf("expected uncertain commit retaining transport cause, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("client did not report lost COMMIT response")
	}
	assertUploadCounts(t, pool, 1, 3, 3)
	stored, err := repository.NewStorageRepository(pool).GetByKey(ctx, image.OriginalKey)
	if err != nil || stored.State != repository.StorageStateRetained {
		t.Fatalf("committed intent was discarded: %+v, %v", stored, err)
	}
}

func TestUploadRejectsJobIntent(t *testing.T) {
	pool := migratedReadPool(t)
	insertReadImage(t, pool, 1, readCreatedAt, false)
	jobs := insertReadJobs(t, pool)
	image := uploadInput()
	if _, err := pool.Exec(context.Background(), `INSERT INTO storage_objects(object_key,image_id,job_id,attempt_token,write_until)
 VALUES ($1,$2,$3,$4,clock_timestamp()+interval '1 minute')`, image.OriginalKey, image.ID, jobs[1].ID, jobs[1].AttemptToken); err != nil {
		t.Fatal(err)
	}
	registry := repository.NewStorageRepository(pool)
	before, err := registry.GetByKey(context.Background(), image.OriginalKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploadRepo(t, pool).Create(context.Background(), image); !errors.Is(err, repository.ErrInvalidWriteIntent) {
		t.Fatalf("expected original-only rejection, got %v", err)
	}
	after, err := registry.GetByKey(context.Background(), image.OriginalKey)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("job intent was modified")
	}
	assertUploadCounts(t, pool, 1, 3, 0)
}
