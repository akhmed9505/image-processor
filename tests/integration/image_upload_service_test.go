//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	objectstorage "github.com/akhmed9505/image-processor/internal/infra/storage"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func uploadStorage(t *testing.T) *objectstorage.Storage {
	t.Helper()
	cfg := minioConfig(t)
	s, e := objectstorage.New(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for obj := range raw.ListObjects(ctx, cfg.Bucket, minio.ListObjectsOptions{Recursive: true}) {
			if obj.Err != nil {
				t.Error(obj.Err)
				continue
			}
			if e := raw.RemoveObject(ctx, cfg.Bucket, obj.Key, minio.RemoveObjectOptions{}); e != nil {
				t.Error(e)
			}
		}
		if e := raw.RemoveBucket(ctx, cfg.Bucket); e != nil {
			t.Error(e)
		}
		s.Close()
	})
	return s
}
func uploadBytes(t *testing.T, format string) []byte {
	t.Helper()
	var b bytes.Buffer
	var e error
	src := image.NewNRGBA(image.Rect(0, 0, 120, 80))
	if format == "jpeg" {
		e = jpeg.Encode(&b, src, nil)
	} else {
		e = png.Encode(&b, src)
	}
	if e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func uploadOperations() []service.OperationRequest {
	return []service.OperationRequest{{Type: domain.OperationResize, Params: domain.OperationParams{Resize: domain.ResizeParams{Width: 60, Height: 40}}}, {Type: domain.OperationThumbnail, Params: domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 20}}}, {Type: domain.OperationWatermark}}
}
func imageUpload(t *testing.T, p *pgxpool.Pool, storage service.ObjectStorage) *service.UploadService {
	t.Helper()
	s, e := service.NewUploadService(uploadRepo(t, p), repository.NewStorageRepository(p), storage, 0)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestImageUploadService(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			p := migratedReadPool(t)
			storage := uploadStorage(t)
			s := imageUpload(t, p, storage)
			ctx := context.Background()
			data := uploadBytes(t, format)
			got, e := s.Upload(ctx, "untrusted/path/wrong-extension.gif", bytes.NewReader(data), uploadOperations())
			if e != nil {
				t.Fatal(e)
			}
			if len(got.Jobs) != 3 || got.Width != 120 || got.Height != 80 || got.ContentType != "image/"+format || got.SizeBytes != int64(len(data)) {
				t.Fatalf("bad uploaded image %+v", got)
			}
			for _, j := range got.Jobs {
				if j.Status != domain.JobStatusPending || j.ImageID != got.ID {
					t.Fatal("incorrect initial job")
				}
			}
			ext := format
			if ext == "jpeg" {
				ext = "jpg"
			}
			if got.OriginalKey != got.ID+"/original."+ext {
				t.Fatal("filename used for key")
			}
			intent, e := repository.NewStorageRepository(p).GetByKey(ctx, got.OriginalKey)
			if e != nil || intent.State != repository.StorageStateRetained || intent.ImageID != got.ID {
				t.Fatal("intent not retained", e)
			}
			if ttl := intent.WriteUntil.Sub(intent.CreatedAt); ttl < 120*time.Second || ttl > 121*time.Second {
				t.Fatalf("TTL %v", ttl)
			}
			body, info, e := storage.Get(ctx, got.OriginalKey)
			if e != nil {
				t.Fatal(e)
			}
			actual, e := io.ReadAll(body)
			body.Close()
			if e != nil || !bytes.Equal(actual, data) || info.ContentType != got.ContentType {
				t.Fatal("original bytes or format changed", e)
			}
			stored, e := repository.NewImageRepository(p).GetByID(ctx, got.ID)
			if e != nil || len(stored.Jobs) != 3 {
				t.Fatal("image not readable", e)
			}
			assertUploadCounts(t, p, 1, 3, 3)
		})
	}
}

type servicePutHook struct {
	storage *objectstorage.Storage
	after   func(context.Context, string) error
}

func (h servicePutHook) Put(ctx context.Context, key string, r io.Reader, size int64, mime string) error {
	if e := h.storage.Put(ctx, key, r, size, mime); e != nil {
		return e
	}
	return h.after(ctx, key)
}
func TestImageUploadServiceFailureCleanup(t *testing.T) {
	for _, kind := range []string{"rollback", "expired intent", "cancel after PUT", "ambiguous PUT"} {
		t.Run(kind, func(t *testing.T) {
			p := migratedReadPool(t)
			storage := uploadStorage(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("PUT response lost")
			if kind == "rollback" {
				if _, e := p.Exec(ctx, `CREATE FUNCTION reject_upload_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected upload failure'; END $$;
 CREATE TRIGGER reject_upload_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_upload_event()`); e != nil {
					t.Fatal(e)
				}
			}
			key := ""
			hook := servicePutHook{storage: storage, after: func(_ context.Context, k string) error {
				key = k
				switch kind {
				case "expired intent":
					_, e := p.Exec(context.Background(), "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 second'")
					return e
				case "cancel after PUT":
					cancel()
				case "ambiguous PUT":
					return cause
				}
				return nil
			}}
			got, e := imageUpload(t, p, hook).Upload(ctx, "x.png", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
			if e == nil || got.ID != "" {
				t.Fatal("failed upload succeeded")
			}
			switch kind {
			case "rollback":
				var pgErr *pgconn.PgError
				if !errors.As(e, &pgErr) || pgErr.Code != "P0001" {
					t.Fatal("SQL cause lost", e)
				}
			case "expired intent":
				if !errors.Is(e, repository.ErrWriteIntentExpired) {
					t.Fatal(e)
				}
			case "cancel after PUT":
				if !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
			case "ambiguous PUT":
				if !errors.Is(e, cause) {
					t.Fatal(e)
				}
			}
			assertUploadCounts(t, p, 0, 0, 0)
			intent, e := repository.NewStorageRepository(p).GetByKey(context.Background(), key)
			if e != nil || intent.State != repository.StorageStateWriting {
				t.Fatal("write intent lost", e)
			}
			if _, e := storage.Stat(context.Background(), key); e != nil {
				t.Fatal("object was unsafely deleted", e)
			}
			cleanup := cleanupRepo(t, p)
			if kind != "expired intent" {
				page, e := cleanup.FindDeletable(context.Background(), 10, "", 0)
				if e != nil || len(page.Objects) != 0 {
					t.Fatal("active write deadline ignored", e)
				}
			}
			if _, e := p.Exec(context.Background(), "UPDATE storage_objects SET write_until=clock_timestamp()-interval '1 minute'"); e != nil {
				t.Fatal(e)
			}
			page, e := cleanup.FindDeletable(context.Background(), 10, "", 0)
			if e != nil || len(page.Objects) != 1 || page.Objects[0].ObjectKey != key {
				t.Fatal("orphan not available to cleanup", e)
			}
		})
	}
}

func TestImageUploadMinIOFailure(t *testing.T) {
	p := migratedReadPool(t)
	cfg := minioConfig(t)
	storage, e := objectstorage.New(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer storage.Close()
	raw, e := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")})
	if e != nil {
		t.Fatal(e)
	}
	if e := raw.RemoveBucket(context.Background(), cfg.Bucket); e != nil {
		t.Fatal(e)
	}
	_, e = imageUpload(t, p, storage).Upload(context.Background(), "x", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
	var response minio.ErrorResponse
	if !errors.As(e, &response) || response.Code != "NoSuchBucket" {
		t.Fatal("MinIO cause lost", e)
	}
	assertUploadCounts(t, p, 0, 0, 0)
	var count int
	if e := p.QueryRow(context.Background(), "SELECT count(*) FROM storage_objects WHERE state='writing'").Scan(&count); e != nil || count != 1 {
		t.Fatal("intent missing", e)
	}
}

func TestImageUploadInvalidOriginal(t *testing.T) {
	p := migratedReadPool(t)
	storage := uploadStorage(t)
	if _, e := imageUpload(t, p, storage).Upload(context.Background(), "fake.png", strings.NewReader("invalid"), uploadOperations()); !errors.Is(e, processor.ErrUnsupportedFormat) {
		t.Fatal(e)
	}
	assertUploadCounts(t, p, 0, 0, 0)
	var count int
	if e := p.QueryRow(context.Background(), "SELECT count(*) FROM storage_objects").Scan(&count); e != nil || count != 0 {
		t.Fatal("intent for invalid original", e)
	}
	page, e := storage.List(context.Background(), "", "", 10)
	if e != nil || len(page.Objects) != 0 {
		t.Fatal("invalid original stored", e)
	}
}

func TestImageUploadUncertainCommit(t *testing.T) {
	p := migratedReadPool(t)
	storage := uploadStorage(t)
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
		return &uploadCommitResponseDrop{Conn: conn, blocked: blocked, release: release, ctx: ctx}, nil
	}
	client, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { cancel(); client.Close() }()
	s, e := service.NewUploadService(uploadRepo(t, client), repository.NewStorageRepository(p), storage, 0)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		_, e := s.Upload(ctx, "x.png", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
		done <- e
	}()
	select {
	case <-blocked:
	case e := <-done:
		t.Fatalf("COMMIT not intercepted: %v", e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	awaitOutboxState(t, ctx, func() (bool, error) {
		var count int
		e := p.QueryRow(ctx, "SELECT count(*) FROM images").Scan(&count)
		return count == 1, e
	})
	close(release)
	select {
	case e := <-done:
		if !errors.Is(e, repository.ErrUploadCommitUncertain) || !errors.Is(e, pgconn.ErrConnClosed) {
			t.Fatal("uncertainty or cause lost", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var id, key string
	if e := p.QueryRow(ctx, "SELECT id::text,original_key FROM images").Scan(&id, &key); e != nil {
		t.Fatal(e)
	}
	intent, e := repository.NewStorageRepository(p).GetByKey(ctx, key)
	if e != nil || intent.State != repository.StorageStateRetained {
		t.Fatal("committed intent changed", e)
	}
	if _, e := storage.Stat(ctx, key); e != nil {
		t.Fatal("committed object deleted", e)
	}
	if got, e := repository.NewImageRepository(p).GetByID(ctx, id); e != nil || len(got.Jobs) != 3 {
		t.Fatal("committed image lost", e)
	}
	assertUploadCounts(t, p, 1, 3, 3)
}
