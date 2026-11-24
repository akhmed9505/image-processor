//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	infrastructure "github.com/akhmed9505/image-processor/internal/infra/postgres"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func migratedReadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root, err := infrastructure.New(ctx, testPostgresConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	var database string
	if err := root.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if database != "imageprocessor_test" {
		t.Fatal("repository tests require the isolated test database")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "image_read_" + hex.EncodeToString(nonce[:])
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := root.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg := root.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg.ConnConfig)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var readCreatedAt = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

func readImageID(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }

func insertReadImage(t *testing.T, pool *pgxpool.Pool, n int, created time.Time, deleted bool) {
	t.Helper()
	var deletedAt any
	if deleted {
		deletedAt = created.Add(time.Minute)
	}
	_, err := pool.Exec(context.Background(), `INSERT INTO images(id,original_name,original_key,content_type,size_bytes,width,height,created_at,deleted_at)
 VALUES ($1,'Фото.png',$2,'image/png',12345,1200,800,$3,$4)`, readImageID(n), fmt.Sprintf("%d/original.png", n), created, deletedAt)
	if err != nil {
		t.Fatal(err)
	}
}

func insertReadJobs(t *testing.T, pool *pgxpool.Pool) []domain.Job {
	t.Helper()
	jobs := []domain.Job{
		{ID: "10000000-0000-0000-0000-000000000001", ImageID: readImageID(1), Operation: domain.OperationResize, Params: domain.OperationParams{Resize: domain.ResizeParams{Width: 600, Height: 600}}, Status: domain.JobStatusCompleted, Attempts: 1, ResultKey: "1/results/resize.png", ResultWidth: 600, ResultHeight: 400, ResultSizeBytes: 1000, CreatedAt: readCreatedAt, UpdatedAt: readCreatedAt.Add(time.Minute), CompletedAt: readCreatedAt.Add(time.Minute)},
		{ID: "10000000-0000-0000-0000-000000000002", ImageID: readImageID(1), Operation: domain.OperationThumbnail, Params: domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 200}}, Status: domain.JobStatusProcessing, Attempts: 2, AttemptToken: tokenID, LeaseUntil: readCreatedAt.Add(90 * time.Second), CreatedAt: readCreatedAt, UpdatedAt: readCreatedAt.Add(time.Minute)},
		{ID: "10000000-0000-0000-0000-000000000003", ImageID: readImageID(1), Operation: domain.OperationWatermark, Status: domain.JobStatusRetrying, Attempts: 3, NextRetryAt: readCreatedAt.Add(2 * time.Minute), LastErrorCode: "storage_unavailable", LastError: "storage unavailable", CreatedAt: readCreatedAt, UpdatedAt: readCreatedAt.Add(time.Minute)},
	}
	params := []string{`{"width":600,"height":600}`, `{"size":200}`, `{}`}
	for i, job := range jobs {
		var token, lease, retry, completed, key, width, height, size, code, message any
		if job.AttemptToken != "" {
			token = job.AttemptToken
			lease = job.LeaseUntil
		}
		if !job.NextRetryAt.IsZero() {
			retry = job.NextRetryAt
		}
		if !job.CompletedAt.IsZero() {
			completed = job.CompletedAt
			key = job.ResultKey
			width = job.ResultWidth
			height = job.ResultHeight
			size = job.ResultSizeBytes
		}
		if job.LastErrorCode != "" {
			code = job.LastErrorCode
			message = job.LastError
		}
		_, err := pool.Exec(context.Background(), `INSERT INTO jobs(id,image_id,operation,params,status,attempts,attempt_token,lease_until,next_retry_at,
   result_key,result_width,result_height,result_size_bytes,last_error_code,last_error,created_at,updated_at,completed_at)
   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, job.ID, job.ImageID, job.Operation, params[i], job.Status, job.Attempts, token, lease, retry, key, width, height, size, code, message, job.CreatedAt, job.UpdatedAt, completed)
		if err != nil {
			t.Fatal(err)
		}
	}
	return jobs
}

func TestImageRepositoryMapping(t *testing.T) {
	pool := migratedReadPool(t)
	insertReadImage(t, pool, 1, readCreatedAt, false)
	wantJobs := insertReadJobs(t, pool)
	repo := repository.NewImageRepository(pool)
	image, err := repo.GetByID(context.Background(), readImageID(1))
	if err != nil {
		t.Fatal(err)
	}
	if image.ID != readImageID(1) || image.OriginalName != "Фото.png" || image.OriginalKey != "1/original.png" || image.ContentType != "image/png" || image.SizeBytes != 12345 || image.Width != 1200 || image.Height != 800 || !image.CreatedAt.Equal(readCreatedAt) || !image.DeletedAt.IsZero() {
		t.Fatalf("unexpected metadata: %+v", image)
	}
	for i := range image.Jobs {
		image.Jobs[i].CreatedAt = image.Jobs[i].CreatedAt.UTC()
		image.Jobs[i].UpdatedAt = image.Jobs[i].UpdatedAt.UTC()
		if !image.Jobs[i].CompletedAt.IsZero() {
			image.Jobs[i].CompletedAt = image.Jobs[i].CompletedAt.UTC()
		}
		if !image.Jobs[i].LeaseUntil.IsZero() {
			image.Jobs[i].LeaseUntil = image.Jobs[i].LeaseUntil.UTC()
		}
		if !image.Jobs[i].NextRetryAt.IsZero() {
			image.Jobs[i].NextRetryAt = image.Jobs[i].NextRetryAt.UTC()
		}
	}
	if !reflect.DeepEqual(image.Jobs, wantJobs) {
		t.Fatalf("jobs mismatch: got %+v, want %+v", image.Jobs, wantJobs)
	}
	page, err := repo.List(context.Background(), 20, nil)
	if err != nil || len(page.Images) != 1 {
		t.Fatalf("list mapping: page=%+v error=%v", page, err)
	}
	if len(page.Images[0].Jobs) != 3 {
		t.Fatal("list did not load the complete job set")
	}
}

func TestImageRepositoryPagination(t *testing.T) {
	pool := migratedReadPool(t)
	for n := 1; n <= 3; n++ {
		insertReadImage(t, pool, n, readCreatedAt, false)
	}
	insertReadImage(t, pool, 4, readCreatedAt.Add(-time.Minute), false)
	insertReadImage(t, pool, 5, readCreatedAt, true)
	insertReadJobs(t, pool)
	repo := repository.NewImageRepository(pool)
	first, err := repo.List(context.Background(), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Images) != 2 || first.Images[0].ID != readImageID(3) || first.Images[1].ID != readImageID(2) || first.NextCursor == nil {
		t.Fatalf("unexpected first page: %+v", first)
	}
	if len(first.Images[0].Jobs) != 0 || len(first.Images[1].Jobs) != 0 {
		t.Fatal("jobs assigned to the wrong image")
	}
	// The cursor is a value, so deleting its image must not break the next page.
	if _, err := pool.Exec(context.Background(), `UPDATE images SET deleted_at=now() WHERE id=$1`, readImageID(2)); err != nil {
		t.Fatal(err)
	}
	insertReadImage(t, pool, 99, readCreatedAt.Add(time.Minute), false)
	second, err := repo.List(context.Background(), 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Images) != 2 || second.Images[0].ID != readImageID(1) || second.Images[1].ID != readImageID(4) || second.NextCursor != nil || len(second.Images[0].Jobs) != 3 || len(second.Images[1].Jobs) != 0 {
		t.Fatalf("unexpected second page: %+v", second)
	}
	empty, err := repo.List(context.Background(), 2, &repository.ImageCursor{CreatedAt: second.Images[1].CreatedAt, ID: second.Images[1].ID})
	if err != nil || len(empty.Images) != 0 || empty.NextCursor != nil {
		t.Fatalf("expected empty final page, got %+v, %v", empty, err)
	}
	for _, id := range []string{readImageID(5), readImageID(999)} {
		if _, err := repo.GetByID(context.Background(), id); !errors.Is(err, domain.ErrImageNotFound) {
			t.Fatalf("expected not found, got %v", err)
		}
	}
	image, err := repo.GetByID(context.Background(), readImageID(4))
	if err != nil || len(image.Jobs) != 0 {
		t.Fatalf("image without jobs: %+v, %v", image, err)
	}
}

func TestImageRepositoryLimits(t *testing.T) {
	pool := migratedReadPool(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO images(id,original_name,original_key,content_type,size_bytes,width,height)
 SELECT gen_random_uuid(),'photo','limit-'||n,'image/jpeg',1,1,1 FROM generate_series(1,101) n`); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewImageRepository(pool)
	for limit, want := range map[int]int{0: 20, 1: 1, 100: 100} {
		page, err := repo.List(context.Background(), limit, nil)
		if err != nil || len(page.Images) != want || page.NextCursor == nil {
			t.Fatalf("limit %d: got %d images, cursor=%v, error=%v", limit, len(page.Images), page.NextCursor, err)
		}
	}
	for _, limit := range []int{-1, 101} {
		if _, err := repo.List(context.Background(), limit, nil); !errors.Is(err, repository.ErrInvalidPagination) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	if _, err := repo.List(context.Background(), 20, &repository.ImageCursor{}); !errors.Is(err, repository.ErrInvalidPagination) {
		t.Fatalf("empty cursor: %v", err)
	}
}

func TestImageRepositoryErrors(t *testing.T) {
	pool := migratedReadPool(t)
	insertReadImage(t, pool, 1, readCreatedAt, false)
	insertReadJobs(t, pool)
	repo := repository.NewImageRepository(pool)
	_, err := repo.GetByID(context.Background(), "not-a-uuid")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22P02" {
		t.Fatalf("expected wrapped UUID error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.GetByID(ctx, readImageID(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := repo.List(ctx, 20, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET params='{"width":0,"height":1}' WHERE operation='resize'`); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetByID(context.Background(), readImageID(1)); !errors.Is(err, domain.ErrInvalidOperationParams) {
		t.Fatalf("invalid stored params: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `ALTER TABLE jobs RENAME TO unavailable_jobs`); err != nil {
		t.Fatal(err)
	}
	_, err = repo.GetByID(context.Background(), readImageID(1))
	if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
		t.Fatalf("expected wrapped job query failure, got %v", err)
	}
	page, err := repo.List(context.Background(), 20, nil)
	if !errors.As(err, &pgErr) || pgErr.Code != "42P01" || len(page.Images) != 0 {
		t.Fatalf("expected query error without partial page, got %+v, %v", page, err)
	}
}

type imageReadTraceKey struct{}
type imageReadBarrier struct {
	used    atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (b *imageReadBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM images") && b.used.CompareAndSwap(false, true) {
		return context.WithValue(ctx, imageReadTraceKey{}, true)
	}
	return ctx
}
func (b *imageReadBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(imageReadTraceKey{}) == true {
		close(b.reached)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
}

func TestImageRepositorySnapshot(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(fmt.Sprintf("list=%v", list), func(t *testing.T) {
			pool := migratedReadPool(t)
			insertReadImage(t, pool, 1, readCreatedAt, false)
			insertReadJobs(t, pool)
			barrier := &imageReadBarrier{reached: make(chan struct{}), release: make(chan struct{})}
			cfg := pool.Config()
			cfg.ConnConfig.Tracer = barrier
			reader, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type result struct {
				image domain.Image
				err   error
			}
			done := make(chan result, 1)
			go func() {
				repo := repository.NewImageRepository(reader)
				if list {
					page, err := repo.List(ctx, 20, nil)
					if err != nil {
						done <- result{err: err}
						return
					}
					if len(page.Images) != 1 {
						done <- result{err: fmt.Errorf("expected one image, got %d", len(page.Images))}
						return
					}
					done <- result{image: page.Images[0]}
				} else {
					image, err := repo.GetByID(ctx, readImageID(1))
					done <- result{image, err}
				}
			}()
			select {
			case <-barrier.reached:
			case <-ctx.Done():
				t.Fatal("reader did not reach snapshot barrier")
			}
			_, err = pool.Exec(ctx, `WITH updated_image AS (UPDATE images SET original_name='changed',deleted_at=now() RETURNING id)
   UPDATE jobs SET status='cancelled',attempt_token=NULL,lease_until=NULL,next_retry_at=NULL,
   completed_at=now(),result_key=NULL,result_width=NULL,result_height=NULL,result_size_bytes=NULL
   WHERE image_id IN (SELECT id FROM updated_image) AND status IN ('pending','processing','retrying')`)
			close(barrier.release)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-done:
				if got.err != nil {
					t.Fatal(got.err)
				}
				if got.image.OriginalName != "Фото.png" || len(got.image.Jobs) != 3 || got.image.Jobs[1].Status != domain.JobStatusProcessing {
					t.Fatalf("mixed snapshots: %+v", got.image)
				}
			case <-ctx.Done():
				t.Fatal("snapshot read timed out")
			}
			if _, err := repository.NewImageRepository(reader).GetByID(ctx, readImageID(1)); !errors.Is(err, domain.ErrImageNotFound) {
				t.Fatalf("subsequent read must observe deletion: %v", err)
			}
		})
	}
}
