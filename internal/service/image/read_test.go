package image

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
)

type imageReaderMock struct {
	get  func(context.Context, string) (domain.Image, error)
	list func(context.Context, int, *ImageCursor) (ImageRecords, error)
}

func (m imageReaderMock) GetByID(c context.Context, id string) (domain.Image, error) {
	return m.get(c, id)
}
func (m imageReaderMock) List(c context.Context, n int, cur *ImageCursor) (ImageRecords, error) {
	return m.list(c, n, cur)
}

type imageDeleterMock func(context.Context, string) error

func (f imageDeleterMock) Delete(c context.Context, id string) error { return f(c, id) }

type contentStorageMock func(context.Context, string) (io.ReadCloser, error)

func (f contentStorageMock) Open(c context.Context, k string) (io.ReadCloser, error) { return f(c, k) }
func readImage() domain.Image {
	return domain.Image{ID: "image", OriginalName: "name.png", OriginalKey: "secret/original", ContentType: "image/png", SizeBytes: 4, Width: 2, Height: 2, CreatedAt: time.Now(), Jobs: []domain.Job{{ID: "job", Operation: domain.OperationWatermark, Status: domain.JobStatusCompleted, ResultKey: "secret/result", AttemptToken: "private-token", ResultWidth: 1, ResultHeight: 1, ResultSizeBytes: 2, LastError: "secret internal cause"}}}
}
func readRepo(t *testing.T, image domain.Image) imageReaderMock {
	t.Helper()
	return imageReaderMock{get: func(ctx context.Context, id string) (domain.Image, error) {
		checkCleanupTimeout(t, ctx, 5*time.Second)
		if id != "image" {
			t.Fatal(id)
		}
		return image, nil
	}}
}

func TestReadMetadataAndList(t *testing.T) {
	image := readImage()
	repo := readRepo(t, image)
	cursor := &ImageCursor{ID: "after", CreatedAt: time.Now()}
	next := &ImageCursor{ID: image.ID, CreatedAt: image.CreatedAt}
	repo.list = func(ctx context.Context, n int, cur *ImageCursor) (ImageRecords, error) {
		checkCleanupTimeout(t, ctx, 5*time.Second)
		if n != 1 || cur != cursor {
			t.Fatal("pagination changed")
		}
		return ImageRecords{Images: []domain.Image{image}, NextCursor: next}, nil
	}
	s := NewReadDeleteService(repo, nil, nil)
	info, e := s.Get(context.Background(), "image")
	if e != nil || info.ID != image.ID || info.Status != domain.ImageStatusCompleted || len(info.Jobs) != 1 || info.Jobs[0].ResultWidth != 1 {
		t.Fatal(info, e)
	}
	page, e := s.List(context.Background(), 1, cursor)
	if e != nil || len(page.Images) != 1 || page.NextCursor != next {
		t.Fatal(page, e)
	}
	encoded, _ := json.Marshal(page)
	for _, secret := range []string{"secret/", "private-token", "secret internal cause", "OriginalKey", "ResultKey", "AttemptToken", "LeaseUntil"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("private field exposed", string(encoded))
		}
	}
	for _, status := range []domain.JobStatus{domain.JobStatusPending, domain.JobStatusProcessing, domain.JobStatusRetrying, domain.JobStatusCompleted, domain.JobStatusFailed} {
		image.Jobs[0].Status = status
		s := NewReadDeleteService(readRepo(t, image), nil, nil)
		info, e := s.Get(context.Background(), "image")
		want, _ := image.ProcessingStatus()
		if e != nil || info.Status != want {
			t.Fatal(info, e)
		}
	}
}

func TestReadMissingAndInvalidStates(t *testing.T) {
	for _, cause := range []error{domain.ErrImageNotFound, context.Canceled, context.DeadlineExceeded, errors.New("postgres failed secret/key")} {
		s := NewReadDeleteService(imageReaderMock{get: func(context.Context, string) (domain.Image, error) { return domain.Image{}, cause }, list: func(context.Context, int, *ImageCursor) (ImageRecords, error) { return ImageRecords{}, cause }}, nil, nil)
		_, e := s.Get(context.Background(), "image")
		if !errors.Is(e, cause) || strings.Contains(e.Error(), "secret/") {
			t.Fatal("get lost cause or leaked key", e)
		}
		if _, e := s.List(context.Background(), 0, nil); !errors.Is(e, cause) {
			t.Fatal(e)
		}
	}
	deleted := readImage()
	deleted.DeletedAt = time.Now()
	s := NewReadDeleteService(readRepo(t, deleted), nil, nil)
	if _, e := s.Get(context.Background(), "image"); !errors.Is(e, domain.ErrImageNotFound) {
		t.Fatal(e)
	}
	if _, e := s.OpenOriginal(context.Background(), "image"); !errors.Is(e, domain.ErrImageNotFound) {
		t.Fatal(e)
	}
	for _, status := range []domain.JobStatus{domain.JobStatusPending, domain.JobStatusProcessing, domain.JobStatusRetrying, domain.JobStatusFailed, domain.JobStatusCancelled} {
		image := readImage()
		image.Jobs[0].Status = status
		s := NewReadDeleteService(readRepo(t, image), nil, nil)
		if _, e := s.OpenResult(context.Background(), "image", "job"); !errors.Is(e, ErrResultNotReady) {
			t.Fatal(status, e)
		}
	}
	s = NewReadDeleteService(readRepo(t, readImage()), nil, nil)
	if _, e := s.OpenResult(context.Background(), "image", "foreign-job"); !errors.Is(e, ErrJobNotFound) {
		t.Fatal(e)
	}
	invalid := readImage()
	invalid.Jobs = nil
	if _, e := NewReadDeleteService(readRepo(t, invalid), nil, nil).Get(context.Background(), "image"); !errors.Is(e, domain.ErrNoJobs) {
		t.Fatal(e)
	}
}

type trackedContent struct {
	io.Reader
	closes   atomic.Int32
	closeErr error
}

func (r *trackedContent) Close() error { r.closes.Add(1); return r.closeErr }
func TestReadStreamOwnership(t *testing.T) {
	for _, result := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "result"}[result], func(t *testing.T) {
			var storageCtx context.Context
			body := &trackedContent{Reader: strings.NewReader("data")}
			s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(ctx context.Context, key string) (io.ReadCloser, error) {
				checkCleanupTimeout(t, ctx, 30*time.Second)
				storageCtx = ctx
				expected := "secret/original"
				if result {
					expected = "secret/result"
				}
				if key != expected {
					t.Fatal(key)
				}
				return body, nil
			}))
			var content Content
			var e error
			if result {
				content, e = s.OpenResult(context.Background(), "image", "job")
			} else {
				content, e = s.OpenOriginal(context.Background(), "image")
			}
			if e != nil || storageCtx.Err() != nil || body.closes.Load() != 0 || content.ContentType != "image/png" {
				t.Fatal(content, e)
			}
			if result && (content.SizeBytes != 2 || content.Width != 1) {
				t.Fatal("wrong result metadata")
			}
			if !result && (content.SizeBytes != 4 || content.Width != 2) {
				t.Fatal("wrong original metadata")
			}
			data, e := io.ReadAll(content.Reader)
			if e != nil || string(data) != "data" || storageCtx.Err() != nil {
				t.Fatal("context ended before read/close", e)
			}
			if e := content.Reader.Close(); e != nil {
				t.Fatal(e)
			}
			_ = content.Reader.Close()
			if storageCtx.Err() != context.Canceled || body.closes.Load() != 1 {
				t.Fatal("stream leaked or closed twice")
			}
		})
	}
}

func TestReadStreamErrorsAndCancellation(t *testing.T) {
	cause := errors.New("network failed /secret/key")
	for _, cause := range []error{cause, ErrContentNotFound, context.Canceled, context.DeadlineExceeded} {
		var streamCtx context.Context
		s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(ctx context.Context, _ string) (io.ReadCloser, error) { streamCtx = ctx; return nil, cause }))
		_, e := s.OpenOriginal(context.Background(), "image")
		if !errors.Is(e, cause) || strings.Contains(e.Error(), "secret/") || streamCtx.Err() != context.Canceled {
			t.Fatal("open error handling", e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &trackedContent{Reader: readFailure{cause}, closeErr: cause}
	s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(context.Context, string) (io.ReadCloser, error) { return body, nil }))
	content, e := s.OpenOriginal(ctx, "image")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := content.Reader.Read(make([]byte, 1)); !errors.Is(e, cause) || strings.Contains(e.Error(), "secret/") {
		t.Fatal(e)
	}
	cancel()
	if _, e := content.Reader.Read(make([]byte, 1)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := content.Reader.Close(); !errors.Is(e, cause) || strings.Contains(e.Error(), "secret/") {
		t.Fatal(e)
	}
}

type readFailure struct{ err error }

func (r readFailure) Read([]byte) (int, error) { return 0, r.err }

func TestReadStreamCloseInterruptsRead(t *testing.T) {
	started := make(chan struct{})
	var streamCtx context.Context
	s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(ctx context.Context, _ string) (io.ReadCloser, error) {
		streamCtx = ctx
		return &trackedContent{Reader: blockedContent{ctx, started}}, nil
	}))
	content, e := s.OpenOriginal(context.Background(), "image")
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := content.Reader.Read(make([]byte, 1)); done <- e }()
	<-started
	_ = content.Reader.Close()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) || streamCtx.Err() != context.Canceled {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked stream leaked")
	}
}

type blockedContent struct {
	ctx     context.Context
	started chan struct{}
}

func (r blockedContent) Read([]byte) (int, error) {
	close(r.started)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestReadDeleteDelegation(t *testing.T) {
	for _, cause := range []error{nil, domain.ErrImageNotFound, context.Canceled, errors.New("uncertain COMMIT")} {
		calls := 0
		s := NewReadDeleteService(nil, imageDeleterMock(func(ctx context.Context, id string) error {
			calls++
			checkCleanupTimeout(t, ctx, 5*time.Second)
			if id != "image" {
				t.Fatal(id)
			}
			return cause
		}), nil)
		for range 2 {
			e := s.Delete(context.Background(), "image")
			if !errors.Is(e, cause) {
				t.Fatal("lost deletion outcome", e)
			}
		}
		if calls != 2 {
			t.Fatal("delete performed additional work")
		}
	}
}

func TestReadStreamDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := make(chan struct{})
	s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(c context.Context, _ string) (io.ReadCloser, error) {
		return &trackedContent{Reader: blockedContent{c, started}}, nil
	}))
	content, e := s.OpenOriginal(ctx, "image")
	if e != nil {
		t.Fatal(e)
	}
	defer content.Reader.Close()
	if _, e := content.Reader.Read(make([]byte, 1)); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("stream deadline lost", e)
	}
}

func TestReadCancelledOpenReleasesBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &trackedContent{Reader: strings.NewReader("data")}
	s := NewReadDeleteService(readRepo(t, readImage()), nil, contentStorageMock(func(context.Context, string) (io.ReadCloser, error) { cancel(); return body, nil }))
	content, e := s.OpenOriginal(ctx, "image")
	if !errors.Is(e, context.Canceled) || content.Reader != nil || body.closes.Load() != 1 {
		t.Fatal("cancelled open leaked response", e)
	}
}
