package storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/minio/minio-go/v7"
)

func TestStorageError(t *testing.T) {
	original := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
	err := storageError("stat", original)
	var response minio.ErrorResponse
	if !errors.Is(err, ErrObjectNotFound) || !errors.As(err, &response) {
		t.Fatal("missing object cause lost")
	}
	if errors.Is(storageError("stat", minio.ErrorResponse{Code: "AccessDenied"}), ErrObjectNotFound) {
		t.Fatal("access error hidden")
	}
	if !errors.Is(storageError("get", context.Canceled), context.Canceled) {
		t.Fatal("context lost")
	}
}

func TestNewFailures(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "public bucket"}[policy], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !policy {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(403)
					w.Write([]byte("<Error><Code>AccessDenied</Code></Error>"))
					return
				}
				if r.Method == "HEAD" {
					return
				}
				if r.URL.Query().Has("location") {
					w.Write([]byte(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
					return
				}
				w.Write([]byte(`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject"}]}`))
			}))
			defer server.Close()
			cfg := config.MinIO{Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "images", AccessKey: "test", SecretKey: "test-secret", Timeout: time.Second}
			if client, err := New(context.Background(), cfg); err == nil {
				client.Close()
				t.Fatal("initialization succeeded")
			}
		})
	}
}

func TestNewContextAndNetwork(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := strings.TrimPrefix(server.URL, "http://")
	server.Close()
	cfg := config.MinIO{Endpoint: endpoint, Bucket: "images", AccessKey: "test", SecretKey: "test-secret", Timeout: 100 * time.Millisecond}
	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("closed endpoint accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(ctx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatal("context cause lost", err)
	}
}
