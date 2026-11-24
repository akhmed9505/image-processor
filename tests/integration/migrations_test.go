//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/infra/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func TestMigrations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := postgres.New(ctx, testPostgresConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if database != "imageprocessor_test" {
		t.Fatal("migrations require the isolated test database")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "migrations_" + hex.EncodeToString(nonce[:])
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanup, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	connConfig := pool.Config().ConnConfig.Copy()
	connConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(1)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := provider.Up(ctx)
	if err != nil || len(applied) != 1 {
		t.Fatalf("Up: migrations=%d error=%v", len(applied), err)
	}
	if version, err := provider.GetDBVersion(ctx); err != nil || version != 1 {
		t.Fatalf("version=%d error=%v", version, err)
	}
	first := schemaSignature(t, ctx, db)
	verifyConstraints(t, ctx, db)
	if applied, err := provider.Up(ctx); err != nil || len(applied) != 0 {
		t.Fatalf("repeated Up: migrations=%d error=%v", len(applied), err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if version, err := provider.GetDBVersion(ctx); err != nil || version != 0 {
		t.Fatalf("Down version=%d error=%v", version, err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name IN ('images','jobs','outbox','storage_objects')`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("Down: remaining tables=%d error=%v", remaining, err)
	}
	if applied, err := provider.Up(ctx); err != nil || len(applied) != 1 {
		t.Fatalf("reapply: migrations=%d error=%v", len(applied), err)
	}
	if second := schemaSignature(t, ctx, db); !reflect.DeepEqual(first, second) {
		t.Fatal("schema changed after Down/Up")
	}
}

func schemaSignature(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
 SELECT table_name||':'||column_name||':'||data_type||':'||is_nullable||':'||coalesce(column_default,'')
 FROM information_schema.columns WHERE table_schema=current_schema()
 UNION ALL
 SELECT tablename||':'||indexname||':'||indexdef FROM pg_indexes WHERE schemaname=current_schema()
 UNION ALL
 SELECT conrelid::regclass::text||':'||conname||':'||pg_get_constraintdef(oid)
 FROM pg_constraint WHERE connamespace=current_schema()::regnamespace
 ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		result = append(result, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

const imageID = "00000000-0000-0000-0000-000000000001"
const jobID = "00000000-0000-0000-0000-000000000002"
const tokenID = "00000000-0000-0000-0000-000000000004"

func seedMigrationRows(t *testing.T, ctx context.Context, tx *sql.Tx) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, `INSERT INTO images(id,original_name,original_key,content_type,size_bytes,width,height) VALUES ($1,'photo.png','original.png','image/png',100,10,10)`, imageID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(id,image_id,operation,params) VALUES ($1,$2,$3,'{"width":600,"height":600}')`, jobID, imageID, domain.OperationResize); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key,dedupe_key) VALUES ('00000000-0000-0000-0000-000000000003',$1,'image.processing.requested','{}','image.processing.jobs',$2,'event-1')`, jobID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_objects(object_key,image_id,write_until) VALUES ('original.png',$1,now()+interval '2 minutes')`, imageID); err != nil {
		t.Fatal(err)
	}
}

func verifyConstraints(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, tc := range []struct{ name, sql, code string }{
		{"image PK", `INSERT INTO images SELECT * FROM images`, "23505"},
		{"original key unique", `INSERT INTO images SELECT '00000000-0000-0000-0000-000000000009',original_name,original_key,content_type,size_bytes,width,height,created_at,deleted_at,cleanup_after,cleanup_lease_until,cleanup_token,last_cleanup_at FROM images`, "23505"},
		{"format", `UPDATE images SET content_type='image/gif'`, "23514"},
		{"positive dimensions", `UPDATE images SET width=0`, "23514"},
		{"positive file size", `UPDATE images SET size_bytes=-1`, "23514"},
		{"metadata not null", `UPDATE images SET original_name=NULL`, "23502"},
		{"cleanup needs tombstone", `UPDATE images SET cleanup_after=now()`, "23514"},
		{"cleanup claim pair", `UPDATE images SET deleted_at=now(),cleanup_token='00000000-0000-0000-0000-000000000004'`, "23514"},
		{"job image FK", `UPDATE jobs SET image_id='00000000-0000-0000-0000-000000000009'`, "23503"},
		{"restrict image delete", `DELETE FROM images`, "23001"},
		{"job operation unique", `INSERT INTO jobs(id,image_id,operation,params) SELECT '00000000-0000-0000-0000-000000000009',image_id,operation,params FROM jobs`, "23505"},
		{"operation", `UPDATE jobs SET operation='rotate'`, "23514"},
		{"params object", `UPDATE jobs SET params='[]'`, "23514"},
		{"status", `UPDATE jobs SET status='unknown'`, "23514"},
		{"attempts", `UPDATE jobs SET attempts=-1`, "23514"},
		{"processing needs lease", `UPDATE jobs SET status='processing',attempts=1,attempt_token='00000000-0000-0000-0000-000000000004'`, "23514"},
		{"pending cannot hold lease", `UPDATE jobs SET lease_until=now()`, "23514"},
		{"retry needs schedule", `UPDATE jobs SET status='retrying'`, "23514"},
		{"terminal needs timestamp", `UPDATE jobs SET status='failed'`, "23514"},
		{"completed needs result", `UPDATE jobs SET status='completed',completed_at=now()`, "23514"},
		{"completed partial metadata", `UPDATE jobs SET status='completed',completed_at=now(),result_key='result',result_width=10,result_height=10`, "23514"},
		{"pending cannot hold result", `UPDATE jobs SET result_key='result'`, "23514"},
		{"outbox job FK", `UPDATE outbox SET job_id='00000000-0000-0000-0000-000000000009'`, "23503"},
		{"restrict job delete", `DELETE FROM jobs`, "23001"},
		{"outbox payload object", `UPDATE outbox SET payload='null'`, "23514"},
		{"requested event needs job", `UPDATE outbox SET job_id=NULL`, "23514"},
		{"published outbox cannot hold claim", `UPDATE outbox SET published_at=now(),lease_until=now(),claim_token='00000000-0000-0000-0000-000000000004'`, "23514"},
		{"storage deadline required", `UPDATE storage_objects SET write_until=NULL`, "23502"},
		{"outbox attempts", `UPDATE outbox SET publish_attempts=-1`, "23514"},
		{"outbox claim pair", `UPDATE outbox SET lease_until=now()`, "23514"},
		{"outbox pending request unique", `INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key) SELECT '00000000-0000-0000-0000-000000000009',job_id,event_type,payload,topic,message_key FROM outbox`, "23505"},
		{"dedupe key unique", `INSERT INTO outbox(id,event_type,payload,topic,message_key,dedupe_key) VALUES ('00000000-0000-0000-0000-000000000009','invalid.message','{}','dlq','key','event-1')`, "23505"},
		{"storage PK", `INSERT INTO storage_objects SELECT * FROM storage_objects`, "23505"},
		{"storage state", `UPDATE storage_objects SET state='unknown'`, "23514"},
		{"storage job FK", `UPDATE storage_objects SET job_id='00000000-0000-0000-0000-000000000009',attempt_token='00000000-0000-0000-0000-000000000004'`, "23503"},
		{"storage attempt pair", `UPDATE storage_objects SET attempt_token='00000000-0000-0000-0000-000000000004'`, "23514"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			seedMigrationRows(t, ctx, tx)
			_, err = tx.ExecContext(ctx, tc.sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != tc.code {
				t.Fatalf("expected SQLSTATE %s, got %v", tc.code, err)
			}
		})
	}
	t.Run("valid states and intents", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		seedMigrationRows(t, ctx, tx)
		for _, state := range []domain.JobStatus{domain.JobStatusProcessing, domain.JobStatusRetrying, domain.JobStatusFailed, domain.JobStatusCancelled, domain.JobStatusCompleted} {
			_, err := tx.ExecContext(ctx, `UPDATE jobs SET status=$1,attempts=1,
    attempt_token=CASE WHEN $1='processing' THEN $2::uuid END,
    lease_until=CASE WHEN $1='processing' THEN now()+interval '90 seconds' END,
    next_retry_at=CASE WHEN $1='retrying' THEN now()+interval '5 seconds' END,
    completed_at=CASE WHEN $1 IN ('completed','failed','cancelled') THEN now() END,
    result_key=CASE WHEN $1='completed' THEN 'result.png' END,
    result_width=CASE WHEN $1='completed' THEN 10 END,
    result_height=CASE WHEN $1='completed' THEN 10 END,
    result_size_bytes=CASE WHEN $1='completed' THEN 100 END`, state, tokenID)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, operation := range []domain.Operation{domain.OperationThumbnail, domain.OperationWatermark} {
			params := `{}`
			if operation == domain.OperationThumbnail {
				params = `{"size":200}`
			}
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET operation=$1,params=$2`, operation, params); err != nil {
				t.Fatal(err)
			}
		}
		for _, statement := range []string{
			`UPDATE outbox SET published_at=now()`,
			`INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key) SELECT '00000000-0000-0000-0000-000000000009',job_id,event_type,payload,topic,message_key FROM outbox`,
			`INSERT INTO storage_objects(object_key,image_id,write_until) VALUES ('uncommitted-original','00000000-0000-0000-0000-000000000008',now()+interval '2 minutes')`,
			`INSERT INTO storage_objects(object_key,image_id,job_id,attempt_token,write_until) SELECT 'result.png',image_id,id,'00000000-0000-0000-0000-000000000004',now()+interval '90 seconds' FROM jobs`,
			`UPDATE images SET deleted_at=now(),cleanup_after=now()`,
			`UPDATE storage_objects SET state='discarded'`,
			`DELETE FROM storage_objects`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	})
}
