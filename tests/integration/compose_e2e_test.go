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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Run after building images: TEST_COMPOSE_E2E=1 go test -tags=integration ./tests/integration -run TestComposeStack.
func TestComposeStack(t *testing.T) {
	if os.Getenv("TEST_COMPOSE_E2E") != "1" {
		t.Skip("requires explicitly enabled full Compose stack test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	freePort := func() string {
		t.Helper()
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		defer l.Close()
		return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
	}
	project := "imageprocessor-compose-test-" + uuid.NewString()[:8]
	env := map[string]string{"POSTGRES_DB": "imageprocessor_test", "POSTGRES_USER": "integration", "POSTGRES_PASSWORD": "integration-postgres-password", "POSTGRES_PORT": freePort(), "MINIO_ACCESS_KEY": "integration-user", "MINIO_SECRET_KEY": "integration-password", "MINIO_BUCKET": "images", "MINIO_PORT": freePort(), "MINIO_CONSOLE_PORT": freePort(), "KAFKA_PORT": freePort(), "HTTP_PORT": freePort(), "KAFKA_JOBS_TOPIC": "image.processing.jobs", "KAFKA_DLQ_TOPIC": "image.processing.dlq", "KAFKA_CONSUMER_GROUP": "image-processor-workers"}
	var lines []string
	commandEnv := append([]string{}, os.Environ()...)
	for key, value := range env {
		lines = append(lines, key+"="+value)
		commandEnv = append(commandEnv, key+"="+value)
	}
	envFile := filepath.Join(t.TempDir(), "compose.env")
	if e := os.WriteFile(envFile, []byte(strings.Join(lines, "\n")), 0600); e != nil {
		t.Fatal(e)
	}
	compose := func(c context.Context, args ...string) []byte {
		t.Helper()
		all := append([]string{"compose", "--project-name", project, "--env-file", envFile}, args...)
		cmd := exec.CommandContext(c, "docker", all...)
		cmd.Dir = "../.."
		cmd.Env = commandEnv
		output, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("compose %v: %v %s", args, e, output)
		}
		return output
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 70*time.Second)
		defer stop()
		// Remove only volumes owned by this new, uniquely named test project.
		compose(cleanup, "down", "--volumes", "--timeout", "60")
	}()
	compose(ctx, "up", "-d", "--wait", "--wait-timeout", "90", "--no-build")
	base := "http://127.0.0.1:" + env["HTTP_PORT"]
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
	for path, content := range map[string]string{"/": "<html lang=\"ru\">", "/app.js": "FormData", "/styles.css": ".workspace"} {
		if status, data := request("GET", path, nil, ""); status != 200 || !bytes.Contains(data, []byte(content)) {
			t.Fatal("web asset missing from application image", path, status)
		}
	}
	results := map[string]map[string][]byte{}
	originals := map[string][]byte{}
	for _, format := range []string{"jpeg", "png"} {
		original := uploadBytes(t, format)
		body, mime := apiMultipart(t, original, `[{"type":"resize","width":60,"height":40},{"type":"thumbnail","size":20},{"type":"watermark"}]`)
		status, data := request("POST", "/upload", body, mime)
		if status != 202 {
			t.Fatal(status, string(data))
		}
		var info struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if e := json.Unmarshal(data, &info); e != nil {
			t.Fatal(e)
		}
		awaitOutboxState(t, ctx, func() (bool, error) {
			status, data := request("GET", "/image/"+info.ID+"/status", nil, "")
			if status != 200 {
				return false, fmt.Errorf("status %d", status)
			}
			e := json.Unmarshal(data, &info)
			return info.Status == "completed", e
		})
		results[info.ID] = map[string][]byte{}
		originals[info.ID] = original
		for op, size := range map[string][2]int{"resize": {60, 40}, "thumbnail": {20, 20}, "watermark": {120, 80}} {
			status, data := request("GET", "/image/"+info.ID+"?operation="+op, nil, "")
			if status != 200 {
				t.Fatal(status)
			}
			decoded, detected, e := image.Decode(bytes.NewReader(data))
			if e != nil || detected != format || decoded.Bounds().Dx() != size[0] || decoded.Bounds().Dy() != size[1] {
				t.Fatal(op, detected, e)
			}
			results[info.ID][op] = data
		}
	}
	offsetSnapshot := func() string {
		t.Helper()
		output := compose(ctx, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", "image.processing.jobs", "--time", "-1")
		var rows []string
		total := 0
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "image.processing.jobs:") {
				rows = append(rows, line)
				fields := strings.Split(line, ":")
				n, e := strconv.Atoi(fields[len(fields)-1])
				if e != nil {
					t.Fatal(e)
				}
				total += n
			}
		}
		if len(rows) != 6 || total < 6 {
			t.Fatal("Kafka notifications missing", string(output))
		}
		sort.Strings(rows)
		return strings.Join(rows, "\n")
	}
	kafkaBefore := offsetSnapshot()
	// Full stop verifies clean server/worker exits and releases PostgreSQL before restart.
	compose(ctx, "stop", "--timeout", "60")
	for _, service := range []string{"server", "worker"} {
		id := strings.TrimSpace(string(compose(ctx, "ps", "-a", "-q", service)))
		output, e := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.ExitCode}} {{.State.OOMKilled}}", id).Output()
		if e != nil || strings.TrimSpace(string(output)) != "0 false" {
			t.Fatal("unclean application stop", service, string(output), e)
		}
	}
	compose(ctx, "down", "--timeout", "60")
	compose(ctx, "up", "-d", "--wait", "--wait-timeout", "90", "--no-build")
	if after := offsetSnapshot(); after != kafkaBefore {
		t.Fatal("Kafka records lost across container recreation", kafkaBefore, after)
	}
	for id, operations := range results {
		for op, expected := range operations {
			status, data := request("GET", "/image/"+id+"?operation="+op, nil, "")
			if status != 200 || !bytes.Equal(expected, data) {
				t.Fatal("result not preserved", id, op, status)
			}
		}
		if status, data := request("GET", "/image/"+id+"/original", nil, ""); status != 200 || !bytes.Equal(data, originals[id]) {
			t.Fatal("original not preserved")
		}
		for range 2 {
			if status, _ := request("DELETE", "/image/"+id, nil, ""); status != 202 {
				t.Fatal(status)
			}
		}
		if status, _ := request("GET", "/image/"+id+"/status", nil, ""); status != 404 {
			t.Fatal("deleted image visible", status)
		}
	}
	t.Log("JPEG/PNG: six operations, downloads, persisted restart, idempotent deletion, clean signal shutdown passed")
}
