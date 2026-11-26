package app

import (
	"context"
	"errors"
	"os"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/akhmed9505/image-processor/internal/infra/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

func migrationProvider(pool *pgxpool.Pool, dir string) (*goose.Provider, error) {
	db := stdlib.OpenDB(*pool.Config().ConnConfig.Copy())
	db.SetMaxOpenConns(1)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		db.Close()
		return nil, err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS(dir), goose.WithDisableGlobalRegistry(true), goose.WithSessionLocker(locker))
	if err != nil {
		db.Close()
	}
	return provider, err
}
func checkSchema(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	provider, err := migrationProvider(pool, dir)
	if err != nil {
		return err
	}
	defer provider.Close()
	sources := provider.ListSources()
	if len(sources) == 0 {
		return errors.New("no migrations found")
	}
	var version int64
	if err := pool.QueryRow(ctx, "SELECT COALESCE(max(version_id),0) FROM goose_db_version WHERE is_applied").Scan(&version); err != nil {
		return err
	}
	if version != sources[len(sources)-1].Version {
		return errors.New("database schema does not match application migrations")
	}
	return nil
}

// Migrate applies Goose migrations explicitly, before either application is started.
func Migrate(ctx context.Context, cfg config.Config) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	pool, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()
	provider, err := migrationProvider(pool, cfg.MigrationsDir)
	if err != nil {
		return err
	}
	defer provider.Close()
	_, err = provider.Up(ctx)
	return err
}
