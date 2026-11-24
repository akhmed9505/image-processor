package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
)

func TestFormatsAndLevels(t *testing.T) {
	levels := []struct {
		name  string
		value slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
	}
	for _, format := range []string{"text", "json"} {
		for threshold, configured := range levels {
			t.Run(format+"/"+configured.name, func(t *testing.T) {
				var output bytes.Buffer
				log, err := New(config.Log{Level: configured.name, Format: format}, &output)
				if err != nil {
					t.Fatal(err)
				}
				log = log.With("service", "worker")
				for _, level := range levels {
					log.Log(context.Background(), level.value, "processing image", "job_id", "job-123")
				}
				lines := strings.Split(strings.TrimSpace(output.String()), "\n")
				if len(lines) != len(levels)-threshold {
					t.Fatalf("got %d records, want %d", len(lines), len(levels)-threshold)
				}
				for i, line := range lines {
					wantLevel := levels[threshold+i].value.String()
					if format == "json" {
						var record struct {
							Time    time.Time `json:"time"`
							Level   string    `json:"level"`
							Message string    `json:"msg"`
							Service string    `json:"service"`
							JobID   string    `json:"job_id"`
						}
						if err := json.Unmarshal([]byte(line), &record); err != nil {
							t.Fatal(err)
						}
						if record.Time.IsZero() || record.Level != wantLevel || record.Message != "processing image" || record.Service != "worker" || record.JobID != "job-123" {
							t.Fatalf("unexpected JSON record: %s", line)
						}
					} else {
						for _, field := range []string{"time=", "level=" + wantLevel, `msg="processing image"`, "service=worker", "job_id=job-123"} {
							if !strings.Contains(line, field) {
								t.Errorf("text record missing %q: %s", field, line)
							}
						}
					}
				}
			})
		}
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, cfg := range []config.Log{
		{Level: "", Format: "json"},
		{Level: "verbose", Format: "json"},
		{Level: "info", Format: ""},
		{Level: "info", Format: "xml"},
	} {
		t.Run(cfg.Level+"/"+cfg.Format, func(t *testing.T) {
			var output bytes.Buffer
			log, err := New(cfg, &output)
			if err == nil || log != nil {
				t.Fatal("expected an error and no logger")
			}
		})
	}
}

func TestDefaultLoggerUnchanged(t *testing.T) {
	original := slog.Default()
	var output bytes.Buffer
	if _, err := New(config.Log{Level: "info", Format: "json"}, &output); err != nil {
		t.Fatal(err)
	}
	if slog.Default() != original {
		t.Fatal("constructor replaced the default logger")
	}
}
