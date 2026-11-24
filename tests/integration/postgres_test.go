//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/akhmed9505/image-processor/internal/infra/postgres"
	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgconn"
)

// These tests only connect and execute SELECT; they never alter database data.
func TestPostgres(t *testing.T) {
	cfg := testPostgresConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1: value=%d error=%v", one, err)
	}
	if pool.Config().MaxConns != cfg.MaxConns {
		t.Fatal("pool limit not configured")
	}
	pool.Close()
	if pool.Stat().TotalConns() != 0 {
		t.Fatal("connections not released")
	}
}

func TestPostgresWrongPassword(t *testing.T) {
	cfg := testPostgresConfig(t)
	cfg.Password = "wrong-integration-password"
	pool, err := postgres.New(context.Background(), cfg)
	if pool != nil {
		pool.Close()
		t.Fatal("unexpected pool after authentication failure")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
		t.Fatalf("expected invalid_password SQLSTATE 28P01, got %v", err)
	}
	if strings.Contains(err.Error(), cfg.Password) {
		t.Fatal("error discloses password")
	}
}

func testPostgresConfig(t *testing.T) config.Postgres {
	t.Helper()
	var settings struct {
		Port     uint16 `env:"TEST_POSTGRES_PORT" envDefault:"55434"`
		User     string `env:"TEST_POSTGRES_USER" envDefault:"imageprocessor_test"`
		Password string `env:"TEST_POSTGRES_PASSWORD" envDefault:"integration-only-password"`
	}
	if err := env.Parse(&settings); err != nil {
		t.Fatal("invalid test PostgreSQL settings")
	}
	return config.Postgres{Host: "127.0.0.1", Port: settings.Port, Database: "imageprocessor_test", User: settings.User, Password: settings.Password, MaxConns: 2, Timeout: 3 * time.Second}
}
