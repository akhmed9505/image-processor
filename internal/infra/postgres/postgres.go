// Package postgres initializes PostgreSQL connections.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a pool and verifies connectivity within cfg.Timeout.
// Connections use plaintext for the local Compose environment.
// The caller must close the returned pool.
func New(ctx context.Context, cfg config.Postgres) (*pgxpool.Pool, error) {
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	dsn := url.URL{Scheme: "postgres", Host: address, User: url.UserPassword(cfg.User, cfg.Password), Path: "/" + cfg.Database, RawQuery: "sslmode=disable"}
	poolConfig, err := pgxpool.ParseConfig(dsn.String())
	if err != nil {
		return nil, errors.New("configure PostgreSQL pool: invalid connection settings")
	}
	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.ConnConfig.ConnectTimeout = cfg.Timeout
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return pool, nil
}
