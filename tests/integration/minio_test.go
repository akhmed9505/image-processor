//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	adapter "github.com/akhmed9505/image-processor/internal/infra/storage"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func minioConfig(t *testing.T) config.MinIO {
	t.Helper()
	get := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	return config.MinIO{Endpoint: net.JoinHostPort("127.0.0.1", get("TEST_MINIO_PORT", "59000")), Bucket: "test-" + uuid.NewString(), AccessKey: get("TEST_MINIO_ACCESS_KEY", "integration-user"), SecretKey: get("TEST_MINIO_SECRET_KEY", "integration-password"), Timeout: 5 * time.Second}
}
func TestMinIOStorage(t *testing.T) {
	cfg := minioConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, e := adapter.New(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	raw, e := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		for obj := range raw.ListObjects(context.Background(), cfg.Bucket, minio.ListObjectsOptions{Recursive: true}) {
			if obj.Err == nil {
				raw.RemoveObject(context.Background(), cfg.Bucket, obj.Key, minio.RemoveObjectOptions{})
			}
		}
		raw.RemoveBucket(context.Background(), cfg.Bucket)
	}()
	again, e := adapter.New(ctx, cfg)
	if e != nil {
		t.Fatal("repeat initialization", e)
	}
	again.Close()
	content := bytes.Repeat([]byte("image-stream"), 1000000)
	if e := s.Put(ctx, "prefix/a.png", bytes.NewReader(content), -1, "image/png"); e != nil {
		t.Fatal(e)
	}
	body, info, e := s.Get(ctx, "prefix/a.png")
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(body)
	body.Close()
	if e != nil || !bytes.Equal(content, got) || info.Size != int64(len(content)) || info.ContentType != "image/png" {
		t.Fatal("stream mismatch", e)
	}
	streamCtx, stopStream := context.WithCancel(ctx)
	stream, _, e := s.Get(streamCtx, "prefix/a.png")
	if e != nil {
		t.Fatal(e)
	}
	first := make([]byte, 1)
	if _, e := stream.Read(first); e != nil {
		t.Fatal(e)
	}
	stopStream()
	_, e = io.Copy(io.Discard, stream)
	stream.Close()
	if !errors.Is(e, context.Canceled) {
		t.Fatal("stream cancellation", e)
	}
	stat, e := s.Stat(ctx, "prefix/a.png")
	if e != nil || stat.Size != info.Size || stat.ETag == "" {
		t.Fatal("metadata", e)
	}
	resp, e := http.Get("http://" + cfg.Endpoint + "/" + cfg.Bucket + "/prefix/a.png")
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("anonymous access %d", resp.StatusCode)
	}
	for _, key := range []string{"prefix/b.png", "prefix/c.png", "other/x"} {
		if e := s.Put(ctx, key, strings.NewReader("x"), 1, "image/png"); e != nil {
			t.Fatal(e)
		}
	}
	cursor := ""
	count := 0
	for {
		page, e := s.List(ctx, "prefix/", cursor, 2)
		if e != nil {
			t.Fatal(e)
		}
		for _, obj := range page.Objects {
			if !strings.HasPrefix(obj.Key, "prefix/") || obj.Key <= cursor {
				t.Fatal("prefix or cursor lost")
			}
			count++
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if count != 3 {
		t.Fatalf("listed %d", count)
	}
	for i := 0; i < 2; i++ {
		if e := s.Delete(ctx, "prefix/a.png"); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.Stat(ctx, "prefix/a.png"); !errors.Is(e, adapter.ErrObjectNotFound) {
		t.Fatal("missing stat", e)
	}
	if body, _, e := s.Get(ctx, "prefix/a.png"); !errors.Is(e, adapter.ErrObjectNotFound) {
		if body != nil {
			body.Close()
		}
		t.Fatal("missing get", e)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if e := s.Put(cancelled, "cancelled", strings.NewReader("x"), 1, "image/png"); !errors.Is(e, context.Canceled) {
		t.Fatal("put cancellation", e)
	}
	if _, _, e := s.Get(cancelled, "prefix/b.png"); !errors.Is(e, context.Canceled) {
		t.Fatal("get cancellation", e)
	}
	if _, e := s.Stat(cancelled, "prefix/b.png"); !errors.Is(e, context.Canceled) {
		t.Fatal("stat cancellation", e)
	}
	if e := s.Delete(cancelled, "prefix/b.png"); !errors.Is(e, context.Canceled) {
		t.Fatal("delete cancellation", e)
	}
	if _, e := s.List(cancelled, "", "", 2); !errors.Is(e, context.Canceled) {
		t.Fatal("list cancellation", e)
	}
}

func TestMinIOConcurrentInitialization(t *testing.T) {
	cfg := minioConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			<-start
			s, e := adapter.New(ctx, cfg)
			if s != nil {
				s.Close()
			}
			done <- e
		}()
	}
	close(start)
	for i := 0; i < 8; i++ {
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	raw, e := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")})
	if e != nil {
		t.Fatal(e)
	}
	if e := raw.RemoveBucket(ctx, cfg.Bucket); e != nil {
		t.Fatal(e)
	}
}
