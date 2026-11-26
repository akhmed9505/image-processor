package image

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
)

type repositoryMock struct {
	create func(context.Context, domain.Image) (domain.Image, error)
}

func (m repositoryMock) Create(ctx context.Context, img domain.Image) (domain.Image, error) {
	return m.create(ctx, img)
}

type registryMock struct {
	reserve func(context.Context, string, string, time.Duration) error
}

func (m registryMock) ReserveOriginal(ctx context.Context, key, id string, ttl time.Duration) error {
	return m.reserve(ctx, key, id, ttl)
}

type storageMock struct {
	put func(context.Context, string, io.Reader, int64, string) error
}

func (m storageMock) Put(ctx context.Context, key string, r io.Reader, size int64, mime string) error {
	return m.put(ctx, key, r, size, mime)
}
func original(t *testing.T, format string) []byte {
	t.Helper()
	var b bytes.Buffer
	src := image.NewNRGBA(image.Rect(0, 0, 120, 80))
	var e error
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
func choices() []OperationRequest {
	return []OperationRequest{{Type: domain.OperationResize, Params: domain.OperationParams{Resize: domain.ResizeParams{Width: 60, Height: 40}}}, {Type: domain.OperationThumbnail, Params: domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 20}}}, {Type: domain.OperationWatermark}}
}

func TestUpload(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			data := original(t, format)
			stage := 0
			var key, id string
			registry := registryMock{func(ctx context.Context, k, i string, ttl time.Duration) error {
				if stage != 0 || ttl != 120*time.Second {
					t.Fatal("intent order or TTL")
				}
				stage++
				key, id = k, i
				if _, e := uuid.Parse(id); e != nil {
					t.Fatal(e)
				}
				return nil
			}}
			storage := storageMock{func(ctx context.Context, k string, r io.Reader, size int64, mime string) error {
				if stage != 1 || k != key || size != int64(len(data)) || mime != "image/"+format {
					t.Fatal("storage arguments or order")
				}
				stage++
				got, e := io.ReadAll(r)
				if e != nil || !bytes.Equal(got, data) {
					t.Fatal("original bytes changed")
				}
				return nil
			}}
			repo := repositoryMock{func(ctx context.Context, img domain.Image) (domain.Image, error) {
				if stage != 2 || img.ID != id || img.OriginalKey != key || img.OriginalName != "../untrusted.jpg" || img.Width != 120 || img.Height != 80 || len(img.Jobs) != 3 {
					t.Fatal("image metadata or sequence")
				}
				stage++
				seen := map[string]bool{id: true}
				for i, j := range img.Jobs {
					if _, e := uuid.Parse(j.ID); e != nil || seen[j.ID] || j.ImageID != id || j.Operation != choices()[i].Type || j.Params != choices()[i].Params {
						t.Fatal("job ID or params")
					}
					seen[j.ID] = true
					img.Jobs[i].Status = domain.JobStatusPending
				}
				img.CreatedAt = time.Now()
				return img, nil
			}}
			s, e := NewUploadService(repo, registry, storage, 0)
			if e != nil {
				t.Fatal(e)
			}
			got, e := s.Upload(context.Background(), "../untrusted.jpg", bytes.NewReader(data), choices())
			if e != nil || stage != 3 || got.CreatedAt.IsZero() || got.Jobs[0].Status != domain.JobStatusPending {
				t.Fatal("upload result", got, e)
			}
			ext := format
			if ext == "jpeg" {
				ext = "jpg"
			}
			if key != id+"/original."+ext {
				t.Fatal("incorrect key")
			}
			assertNoSpools(t)
		})
	}
}
func assertNoSpools(t *testing.T) {
	t.Helper()
	files, e := filepath.Glob(filepath.Join(os.TempDir(), "imageprocessor-upload-*"))
	if e != nil || len(files) != 0 {
		t.Fatal("temporary file leaked", files, e)
	}
}

func TestUploadFailures(t *testing.T) {
	for _, stage := range []string{"register", "put", "create", "cancel-put"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			cause := errors.New("dependency failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := []string{}
			registry := registryMock{func(context.Context, string, string, time.Duration) error {
				calls = append(calls, "register")
				if stage == "register" {
					return cause
				}
				return nil
			}}
			storage := storageMock{func(context.Context, string, io.Reader, int64, string) error {
				calls = append(calls, "put")
				if stage == "put" {
					return cause
				}
				if stage == "cancel-put" {
					cancel()
				}
				return nil
			}}
			repo := repositoryMock{func(context.Context, domain.Image) (domain.Image, error) {
				calls = append(calls, "create")
				return domain.Image{}, cause
			}}
			s, _ := NewUploadService(repo, registry, storage, 0)
			got, e := s.Upload(ctx, "original", bytes.NewReader(original(t, "png")), choices())
			want := cause
			if stage == "cancel-put" {
				want = context.Canceled
			}
			if !errors.Is(e, want) || got.ID != "" {
				t.Fatal("cause or zero result lost", e)
			}
			expected := map[string]int{"register": 1, "put": 2, "create": 3, "cancel-put": 2}[stage]
			if len(calls) != expected {
				t.Fatal("work continued after failure", calls)
			}
			assertNoSpools(t)
		})
	}
}

func TestInvalidRequests(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	s, _ := NewUploadService(repositoryMock{}, registryMock{}, storageMock{}, 0)
	for _, tc := range []struct {
		data       []byte
		operations []OperationRequest
		want       error
	}{
		{[]byte("invalid"), choices(), processor.ErrUnsupportedFormat}, {original(t, "png"), nil, domain.ErrNoJobs}, {original(t, "png"), []OperationRequest{{Type: domain.OperationWatermark}, {Type: domain.OperationWatermark}}, ErrDuplicateOperation}, {original(t, "png"), []OperationRequest{{Type: domain.OperationThumbnail, Params: domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 2049}}}}, processor.ErrLimitExceeded}, {original(t, "png"), []OperationRequest{{Type: domain.OperationResize}}, domain.ErrInvalidOperationParams},
	} {
		if _, e := s.Upload(context.Background(), "x", bytes.NewReader(tc.data), tc.operations); !errors.Is(e, tc.want) {
			t.Fatalf("want %v got %v", tc.want, e)
		}
		assertNoSpools(t)
	}
	if _, e := NewUploadService(nil, nil, nil, -1); e == nil {
		t.Fatal("negative concurrency accepted")
	}
}

type blockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	src     io.Reader
}

func (r *blockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.release })
	return r.src.Read(p)
}
func TestValidationSlotsAndCancellation(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	cause := errors.New("stop after validation")
	registry := registryMock{func(context.Context, string, string, time.Duration) error { return cause }}
	s, _ := NewUploadService(repositoryMock{}, registry, storageMock{}, 0)
	first := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), src: bytes.NewReader(original(t, "png"))}
	done := make(chan error, 1)
	go func() { _, e := s.Upload(context.Background(), "first", first, choices()); done <- e }()
	<-first.started
	second := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), src: bytes.NewReader(original(t, "png"))}
	ctx, cancel := context.WithCancel(context.Background())
	waiting := &waitingContext{Context: ctx, waiting: make(chan struct{})}
	secondDone := make(chan error, 1)
	go func() { _, e := s.Upload(waiting, "second", second, choices()); secondDone <- e }()
	<-waiting.waiting
	select {
	case <-second.started:
		t.Fatal("read input without a validation slot")
	default:
	}
	cancel()
	if e := <-secondDone; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	close(first.release)
	if e := <-done; !errors.Is(e, cause) {
		t.Fatal(e)
	}
	if _, e := s.Upload(context.Background(), "next", bytes.NewReader(original(t, "png")), choices()); !errors.Is(e, cause) {
		t.Fatal("slot not released", e)
	}
	assertNoSpools(t)
}

type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestConfiguredValidationConcurrency(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	cause := errors.New("stop after validation")
	s, _ := NewUploadService(repositoryMock{}, registryMock{func(context.Context, string, string, time.Duration) error { return cause }}, storageMock{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	readers := make([]*blockingReader, 2)
	done := make(chan error, 2)
	for i := range readers {
		r := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), src: bytes.NewReader(original(t, "png"))}
		readers[i] = r
		go func() { _, e := s.Upload(ctx, "original", r, choices()); done <- e }()
	}
	for _, r := range readers {
		select {
		case <-r.started:
		case <-ctx.Done():
			for _, r := range readers {
				close(r.release)
			}
			t.Fatal("configured validation slots were not available")
		}
	}
	for _, r := range readers {
		close(r.release)
	}
	for range readers {
		if e := <-done; !errors.Is(e, cause) {
			t.Fatal(e)
		}
	}
	assertNoSpools(t)
}

func TestUploadInfrastructureContexts(t *testing.T) {
	for _, mode := range []string{"success", "timeout", "parent deadline", "parent cancellation"} {
		for failedStage := 0; failedStage < 3; failedStage++ {
			t.Run(fmt.Sprintf("%s/stage-%d", mode, failedStage), func(t *testing.T) {
				t.Setenv("TMPDIR", t.TempDir())
				data := original(t, "png")
				synctest.Test(t, func(t *testing.T) {
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					if mode == "parent deadline" {
						var stop context.CancelFunc
						parent, stop = context.WithTimeout(parent, time.Second)
						defer stop()
					}
					var calls []context.Context
					call := func(ctx context.Context) error {
						stage := len(calls)
						for _, previous := range calls {
							if previous.Err() != context.Canceled {
								t.Fatal("previous operation context was not released")
							}
						}
						calls = append(calls, ctx)
						limit := 5 * time.Second
						if stage == 1 {
							limit = 30 * time.Second
						}
						want := time.Now().Add(limit)
						if deadline, ok := parent.Deadline(); ok && deadline.Before(want) {
							want = deadline
						}
						if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(want) {
							t.Fatalf("stage %d deadline %v, want %v", stage, deadline, want)
						}
						if stage == failedStage && mode != "success" {
							if mode == "parent cancellation" {
								cancel()
							}
							<-ctx.Done()
							return ctx.Err()
						}
						return nil
					}
					s, _ := NewUploadService(
						repositoryMock{func(ctx context.Context, img domain.Image) (domain.Image, error) { return img, call(ctx) }},
						registryMock{func(ctx context.Context, _, _ string, _ time.Duration) error { return call(ctx) }},
						storageMock{func(ctx context.Context, _ string, _ io.Reader, _ int64, _ string) error { return call(ctx) }}, 1)
					_, err := s.Upload(parent, "input.png", bytes.NewReader(data), choices())
					if mode == "success" {
						if err != nil || len(calls) != 3 || parent.Err() != nil {
							t.Fatal(err, len(calls))
						}
					} else {
						want := context.DeadlineExceeded
						if mode == "parent cancellation" {
							want = context.Canceled
						}
						if !errors.Is(err, want) || len(calls) != failedStage+1 {
							t.Fatal(err, len(calls))
						}
						if mode == "timeout" && parent.Err() != nil {
							t.Fatal("child timeout cancelled parent")
						}
					}
					for _, ctx := range calls {
						if ctx.Err() == nil {
							t.Fatal("operation context leaked")
						}
					}
				})
				assertNoSpools(t)
			})
		}
	}
}
