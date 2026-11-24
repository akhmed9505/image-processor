package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
)

func TestCanceledInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := New(ctx, config.Postgres{Host: "127.0.0.1", Port: 55433, Database: "unused", User: "unused", Password: "unused", MaxConns: 1, Timeout: time.Second})
	if pool != nil {
		pool.Close()
		t.Fatal("failed initialization returned a pool")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
