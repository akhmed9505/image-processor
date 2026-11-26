//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	objectstorage "github.com/akhmed9505/image-processor/internal/infra/storage"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func readDeleteService(p *pgxpool.Pool, s *objectstorage.Storage) *service.ReadDeleteService {
	return service.NewReadDeleteService(repository.NewImageRepository(p), repository.NewDeleteRepository(p), s)
}
func TestImageReadDeleteService(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			p := migratedReadPool(t)
			s := uploadStorage(t)
			ctx := context.Background()
			reader := readDeleteService(p, s)
			data := uploadBytes(t, format)
			image, e := imageUpload(t, p, s).Upload(ctx, "name."+format, bytes.NewReader(data), uploadOperations())
			if e != nil {
				t.Fatal(e)
			}
			info, e := reader.Get(ctx, image.ID)
			if e != nil || info.Status != domain.ImageStatusPending || len(info.Jobs) != 3 {
				t.Fatal(info, e)
			}
			content, e := reader.OpenOriginal(ctx, image.ID)
			if e != nil {
				t.Fatal(e)
			}
			got, e := io.ReadAll(content.Reader)
			closeErr := content.Reader.Close()
			if e != nil || closeErr != nil || !bytes.Equal(got, data) || content.ContentType != "image/"+format || content.SizeBytes != int64(len(data)) {
				t.Fatal("original stream", e, closeErr)
			}
			job := image.Jobs[0]
			if _, e := reader.OpenResult(ctx, image.ID, job.ID); !errors.Is(e, service.ErrResultNotReady) {
				t.Fatal(e)
			}
			if _, e := reader.OpenResult(ctx, image.ID, uuid.NewString()); !errors.Is(e, service.ErrJobNotFound) {
				t.Fatal(e)
			}
			jobs := processor.NewJobService(processingRepo(t, p), s)
			admission, e := jobs.Admit(ctx, job.ID)
			if e != nil {
				t.Fatal(e)
			}
			completed, e := jobs.ProcessAttempt(ctx, admission)
			if e != nil {
				t.Fatal(e)
			}
			info, e = reader.Get(ctx, image.ID)
			if e != nil || info.Status != domain.ImageStatusProcessing {
				t.Fatal(info, e)
			}
			result, e := reader.OpenResult(ctx, image.ID, job.ID)
			if e != nil {
				t.Fatal(e)
			}
			body, _, e := s.Get(ctx, completed.ResultKey)
			if e != nil {
				t.Fatal(e)
			}
			expected, e := io.ReadAll(body)
			_ = body.Close()
			if e != nil {
				t.Fatal(e)
			}
			actual, e := io.ReadAll(result.Reader)
			_ = result.Reader.Close()
			if e != nil || !bytes.Equal(actual, expected) || result.Width != completed.ResultWidth || result.SizeBytes != completed.ResultSizeBytes {
				t.Fatal("result stream", e)
			}
			// An open stream remains caller-owned if its image is tombstoned during reading.
			open, e := reader.OpenOriginal(ctx, image.ID)
			if e != nil {
				t.Fatal(e)
			}
			if e := reader.Delete(ctx, image.ID); e != nil {
				t.Fatal(e)
			}
			if _, e := io.Copy(io.Discard, open.Reader); e != nil {
				t.Fatal("deletion cancelled an existing stream", e)
			}
			_ = open.Reader.Close()
			var before time.Time
			if e := p.QueryRow(ctx, "SELECT cleanup_after FROM images WHERE id=$1", image.ID).Scan(&before); e != nil {
				t.Fatal(e)
			}
			if e := reader.Delete(ctx, image.ID); e != nil {
				t.Fatal("repeated deletion", e)
			}
			var after time.Time
			if e := p.QueryRow(ctx, "SELECT cleanup_after FROM images WHERE id=$1", image.ID).Scan(&after); e != nil || !after.Equal(before) {
				t.Fatal("cleanup schedule reset", e)
			}
			if _, e := reader.Get(ctx, image.ID); !errors.Is(e, domain.ErrImageNotFound) {
				t.Fatal(e)
			}
			if _, e := reader.OpenOriginal(ctx, image.ID); !errors.Is(e, domain.ErrImageNotFound) {
				t.Fatal(e)
			}
			if _, e := reader.OpenResult(ctx, image.ID, job.ID); !errors.Is(e, domain.ErrImageNotFound) {
				t.Fatal(e)
			}
			page, e := reader.List(ctx, 0, nil)
			if e != nil || len(page.Images) != 0 {
				t.Fatal("deleted image listed", e)
			}
			for _, key := range []string{image.OriginalKey, completed.ResultKey} {
				if _, e := s.Stat(ctx, key); e != nil {
					t.Fatal("service physically deleted content", e)
				}
			}
			if e := reader.Delete(ctx, uuid.NewString()); !errors.Is(e, domain.ErrImageNotFound) {
				t.Fatal(e)
			}
		})
	}
}

func TestImageReadServicePaginationAndMissingContent(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx := context.Background()
	reader := readDeleteService(p, s)
	ids := map[string]bool{}
	for range 3 {
		image, e := imageUpload(t, p, s).Upload(ctx, "image", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
		if e != nil {
			t.Fatal(e)
		}
		ids[image.ID] = true
	}
	var cursor *service.ImageCursor
	seen := map[string]bool{}
	for {
		page, e := reader.List(ctx, 1, cursor)
		if e != nil || len(page.Images) != 1 {
			t.Fatal(page, e)
		}
		id := page.Images[0].ID
		if seen[id] || !ids[id] {
			t.Fatal("pagination duplicate or unknown image", id)
		}
		seen[id] = true
		if e := reader.Delete(ctx, id); e != nil {
			t.Fatal(e)
		}
		cursor = page.NextCursor
		if cursor == nil {
			break
		}
	}
	if len(seen) != 3 {
		t.Fatal(seen)
	}
	image, e := imageUpload(t, p, s).Upload(ctx, "missing", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
	if e != nil {
		t.Fatal(e)
	}
	if e := s.Delete(ctx, image.OriginalKey); e != nil {
		t.Fatal(e)
	}
	if _, e := reader.OpenOriginal(ctx, image.ID); !errors.Is(e, service.ErrContentNotFound) {
		t.Fatal("missing content cause lost", e)
	}
	claim := claimJob(t, p, image.Jobs[0].ID)
	if _, e := jobRepo(t, p).Complete(ctx, claim.Job.ID, claim.Job.AttemptToken, resultFor(claim)); e != nil {
		t.Fatal(e)
	}
	if _, e := reader.OpenResult(ctx, image.ID, claim.Job.ID); !errors.Is(e, service.ErrContentNotFound) {
		t.Fatal("missing result cause lost", e)
	}
	if _, e := reader.List(ctx, 101, nil); !errors.Is(e, repository.ErrInvalidPagination) {
		t.Fatal(e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := reader.Get(cancelled, image.ID); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := reader.List(cancelled, 0, nil); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := reader.Delete(cancelled, image.ID); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestImageReadStreamCancellation(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	image, e := imageUpload(t, p, s).Upload(ctx, "image", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
	if e != nil {
		t.Fatal(e)
	}
	content, e := readDeleteService(p, s).OpenOriginal(ctx, image.ID)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	if _, e := content.Reader.Read(make([]byte, 1)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := content.Reader.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestImageDeleteServiceUncertainCommit(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	image, e := imageUpload(t, p, s).Upload(ctx, "image", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
	if e != nil {
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
	reader := readDeleteService(client, s)
	done := make(chan error, 1)
	go func() { done <- reader.Delete(ctx, image.ID) }()
	select {
	case <-blocked:
	case e := <-done:
		t.Fatal("COMMIT not intercepted", e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	awaitOutboxState(t, ctx, func() (bool, error) {
		var deleted bool
		e := p.QueryRow(ctx, "SELECT deleted_at IS NOT NULL FROM images WHERE id=$1", image.ID).Scan(&deleted)
		return deleted, e
	})
	close(release)
	select {
	case e := <-done:
		if !errors.Is(e, repository.ErrDeleteCommitUncertain) || !errors.Is(e, pgconn.ErrConnClosed) {
			t.Fatal("lost uncertain outcome", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e := readDeleteService(p, s).Delete(ctx, image.ID); e != nil {
		t.Fatal("delete not idempotent after uncertainty", e)
	}
	if _, e := s.Stat(ctx, image.OriginalKey); e != nil {
		t.Fatal("uncertain deletion removed content", e)
	}
}
