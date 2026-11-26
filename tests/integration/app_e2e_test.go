//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/infra/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestApplicationEntryPoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pgcfg := testPostgresConfig(t)
	admin, e := postgres.New(ctx, pgcfg)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	database := "e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, e := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if _, e := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{database}.Sanitize()); e != nil {
			t.Error(e)
		}
	}()
	pgcfg.Database = database
	pool, e := postgres.New(ctx, pgcfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	mc := minioConfig(t)
	raw, e := minio.New(mc.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(mc.AccessKey, mc.SecretKey, "")})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for object := range raw.ListObjects(c, mc.Bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err == nil {
				_ = raw.RemoveObject(c, mc.Bucket, object.Key, minio.RemoveObjectOptions{})
			}
		}
		_ = raw.RemoveBucket(c, mc.Bucket)
	}()
	kc := kafkaConfig()
	kadmin := kafkaAdmin(t, kc)
	port, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := port.Addr().String()
	port.Close()
	dir := t.TempDir()
	serverBin := filepath.Join(dir, "server")
	workerBin := filepath.Join(dir, "worker")
	for _, spec := range []struct{ path, pkg string }{{serverBin, "./cmd/server"}, {workerBin, "./cmd/worker"}} {
		cmd := exec.CommandContext(ctx, "go", "build", "-o", spec.path, spec.pkg)
		cmd.Dir = "../.."
		if output, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("build: %s %v", output, e)
		}
	}
	env := os.Environ()
	overrides := map[string]string{"POSTGRES_HOST": pgcfg.Host, "POSTGRES_PORT": strconv.Itoa(int(pgcfg.Port)), "POSTGRES_DB": database, "POSTGRES_USER": pgcfg.User, "POSTGRES_PASSWORD": pgcfg.Password, "POSTGRES_TIMEOUT": "5s", "MINIO_ENDPOINT": mc.Endpoint, "MINIO_BUCKET": mc.Bucket, "MINIO_ACCESS_KEY": mc.AccessKey, "MINIO_SECRET_KEY": mc.SecretKey, "MINIO_SECURE": "false", "KAFKA_BROKERS": strings.Join(kc.Brokers, ","), "KAFKA_JOBS_TOPIC": kc.JobsTopic, "KAFKA_DLQ_TOPIC": kc.DLQTopic, "KAFKA_CONSUMER_GROUP": kc.ConsumerGroup, "KAFKA_TIMEOUT": "10s", "HTTP_ADDRESS": address, "MIGRATIONS_DIR": "migrations", "LOG_LEVEL": "info", "STARTUP_TIMEOUT": "30s"}
	for key, value := range overrides {
		prefix := key + "="
		filtered := env[:0]
		for _, entry := range env {
			if !strings.HasPrefix(entry, prefix) {
				filtered = append(filtered, entry)
			}
		}
		env = append(filtered, prefix+value)
	}
	command := func(binary string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = "../.."
		cmd.Env = env
		return cmd
	}
	// Both entry points must reject an unmigrated database without creating schema.
	for _, bin := range []string{serverBin, workerBin} {
		if e := command(bin).Run(); e == nil {
			t.Fatal("unmigrated schema accepted")
		}
	}
	var missing bool
	if e := pool.QueryRow(ctx, "SELECT to_regclass('images') IS NULL").Scan(&missing); e != nil || !missing {
		t.Fatal("startup modified schema", e)
	}
	if output, e := command(serverBin, "-migrate").CombinedOutput(); e != nil {
		t.Fatalf("migrate: %s %v", output, e)
	}
	if output, e := command(serverBin, "-migrate").CombinedOutput(); e != nil {
		t.Fatalf("repeated migration: %s %v", output, e)
	}
	occupied, e := net.Listen("tcp", address)
	if e != nil {
		t.Fatal(e)
	}
	if e := command(serverBin).Run(); e == nil {
		occupied.Close()
		t.Fatal("occupied HTTP port accepted")
	}
	occupied.Close()
	for _, overrides := range [][]string{{"MINIO_SECRET_KEY=wrong-integration-password"}, {"KAFKA_BROKERS=127.0.0.1:1", "KAFKA_TIMEOUT=100ms"}} {
		cmd := command(serverBin)
		cmd.Env = append(append([]string{}, env...), overrides...)
		if e := cmd.Run(); e == nil {
			t.Fatal("failed infrastructure initialization accepted")
		}
	}

	start := func(bin string, signal os.Signal) func() {
		log, e := os.CreateTemp(dir, "process-*.log")
		if e != nil {
			t.Fatal(e)
		}
		cmd := command(bin)
		cmd.Stdout = log
		cmd.Stderr = log
		if e := cmd.Start(); e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait(); log.Close() }()
		var once sync.Once
		stop := func() {
			once.Do(func() {
				_ = cmd.Process.Signal(signal)
				select {
				case e := <-done:
					if e != nil {
						data, _ := os.ReadFile(log.Name())
						t.Errorf("entry point exit: %v %s", e, data)
					}
				case <-time.After(45 * time.Second):
					cmd.Process.Kill()
					<-done
					t.Error("entry point did not shut down")
				}
			})
		}
		// Deferred explicitly below so process termination precedes database cleanup.
		return stop
	}
	stopServer := start(serverBin, syscall.SIGINT)
	defer stopServer()
	stopWorker := start(workerBin, syscall.SIGTERM)
	defer stopWorker()
	base := "http://" + address
	client := &http.Client{Timeout: 5 * time.Second}
	request := func(method, path string, body io.Reader, mime string) (int, []byte) {
		t.Helper()
		r, e := http.NewRequestWithContext(ctx, method, base+path, body)
		if e != nil {
			t.Fatal(e)
		}
		if mime != "" {
			r.Header.Set("Content-Type", mime)
		}
		response, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		data, e := io.ReadAll(response.Body)
		if e != nil {
			t.Fatal(e)
		}
		return response.StatusCode, data
	}
	awaitOutboxState(t, ctx, func() (bool, error) {
		r, e := client.Get(base + "/images")
		if e != nil {
			return false, nil
		}
		r.Body.Close()
		return r.StatusCode == 200, nil
	})
	for _, format := range []string{"jpeg", "png"} {
		original := uploadBytes(t, format)
		body, mime := apiMultipart(t, original, `[{"type":"resize","width":60,"height":40},{"type":"thumbnail","size":20},{"type":"watermark"}]`)
		status, data := request("POST", "/upload", body, mime)
		if status != 202 {
			t.Fatal(status, string(data))
		}
		var imageInfo struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if e := json.Unmarshal(data, &imageInfo); e != nil {
			t.Fatal(e)
		}
		awaitOutboxState(t, ctx, func() (bool, error) {
			status, data := request("GET", "/image/"+imageInfo.ID+"/status", nil, "")
			if status != 200 {
				return false, fmt.Errorf("status HTTP %d", status)
			}
			e := json.Unmarshal(data, &imageInfo)
			return imageInfo.Status == "completed", e
		})
		for op, dimensions := range map[string][2]int{"resize": {60, 40}, "thumbnail": {20, 20}, "watermark": {120, 80}} {
			status, data := request("GET", "/image/"+imageInfo.ID+"?operation="+op, nil, "")
			if status != 200 {
				t.Fatal(status, string(data))
			}
			decoded, detected, e := image.Decode(bytes.NewReader(data))
			if e != nil || detected != format || decoded.Bounds().Dx() != dimensions[0] || decoded.Bounds().Dy() != dimensions[1] {
				t.Fatal(op, detected, e)
			}
		}
		if status, data := request("GET", "/image/"+imageInfo.ID+"/original", nil, ""); status != 200 || !bytes.Equal(data, original) {
			t.Fatal("original differs", status)
		}
		for range 2 {
			if status, _ := request("DELETE", "/image/"+imageInfo.ID, nil, ""); status != 202 {
				t.Fatal(status)
			}
		}
		if status, _ := request("GET", "/image/"+imageInfo.ID+"/status", nil, ""); status != 404 {
			t.Fatal("deleted image visible", status)
		}
	}
	waitConsumerOffsets(t, ctx, kadmin, kc, 6)
	var completed, published int
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='completed' AND attempts=1").Scan(&completed); e != nil || completed != 6 {
		t.Fatal(completed, e)
	}
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NOT NULL").Scan(&published); e != nil || published != 6 {
		t.Fatal(published, e)
	}
	stopWorker()
	stopServer()
}
