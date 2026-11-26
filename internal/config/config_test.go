package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupConfigENV(t *testing.T) {
	t.Helper()
	for _, name := range []string{"MIGRATIONS_DIR", "HTTP_ADDRESS", "HTTP_READ_HEADER_TIMEOUT", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "STARTUP_TIMEOUT", "VALIDATION_CONCURRENCY", "LOG_LEVEL", "LOG_FORMAT", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_MAX_CONNS", "POSTGRES_TIMEOUT", "MINIO_ENDPOINT", "MINIO_BUCKET", "MINIO_SECURE", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_TIMEOUT", "KAFKA_BROKERS", "KAFKA_JOBS_TOPIC", "KAFKA_DLQ_TOPIC", "KAFKA_CONSUMER_GROUP", "KAFKA_TIMEOUT"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("MINIO_ACCESS_KEY", "test-access-key")
	t.Setenv("MINIO_SECRET_KEY", "test-secret-key")
	t.Setenv("POSTGRES_USER", "test-user")
	t.Setenv("POSTGRES_PASSWORD", "test-password")
}

func writeYAML(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

const kafkaYAML = "\nkafka: {brokers: [localhost:29092], jobs_topic: image.processing.jobs, dlq_topic: image.processing.dlq, consumer_group: image-processor-workers, timeout: 10s}\n"

const minioYAML = "\nminio: {endpoint: localhost:9000, bucket: images, secure: false, timeout: 5s}\n"

const postgresYAML = "\npostgres: {host: localhost, port: 55433, database: imageprocessor, max_conns: 10, timeout: 5s}\n"

func TestLoad(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		overrides  map[string]string
		want       Log
	}{
		{"YAML", "log: {level: warn, format: text}", nil, Log{Level: "warn", Format: "text"}},
		{"ENV priority", "log: {level: info, format: json}", map[string]string{"LOG_LEVEL": "debug", "LOG_FORMAT": "text"}, Log{Level: "debug", Format: "text"}},
		{"partial ENV", "log: {level: error, format: json}", map[string]string{"LOG_FORMAT": "text"}, Log{Level: "error", Format: "text"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupConfigENV(t)
			for name, value := range tc.overrides {
				t.Setenv(name, value)
			}
			cfg, err := Load(writeYAML(t, tc.yaml+postgresYAML+minioYAML+kafkaYAML))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Log != tc.want {
				t.Fatalf("got %+v, want %+v", cfg.Log, tc.want)
			}
		})
	}
}

func TestLoadErrors(t *testing.T) {
	for _, tc := range []struct{ name, yaml, envName, envValue, want string }{
		{name: "empty YAML", yaml: "", want: "parse configuration"},
		{name: "syntax", yaml: "log: [", want: "parse configuration"},
		{name: "unknown root", yaml: "other: true", want: "parse configuration"},
		{name: "unknown nested", yaml: "log: {level: info, format: json, other: true}", want: "parse configuration"},
		{name: "duplicate", yaml: "log: {level: info, level: warn, format: json}", want: "parse configuration"},
		{name: "second document", yaml: "log: {level: info, format: json}\n---\n{}", want: "exactly one"},
		{name: "empty second document", yaml: "log: {level: info, format: json}\n---", want: "exactly one"},
		{name: "wrong type", yaml: "log: {level: [private-value], format: json}", want: "parse configuration"},
		{name: "missing log", yaml: "{}", want: "log.level"},
		{name: "missing format", yaml: "log: {level: info}", want: "log.format"},
		{name: "invalid YAML level", yaml: "log: {level: private-value, format: json}", want: "log.level"},
		{name: "invalid YAML format", yaml: "log: {level: info, format: private-value}", want: "log.format"},
		{name: "invalid ENV level", envName: "LOG_LEVEL", envValue: "private-value", want: "log.level"},
		{name: "invalid ENV format", envName: "LOG_FORMAT", envValue: "private-value", want: "log.format"},
		{name: "empty ENV level", envName: "LOG_LEVEL", want: "LOG_LEVEL"},
		{name: "empty ENV format", envName: "LOG_FORMAT", want: "LOG_FORMAT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupConfigENV(t)
			text := tc.yaml
			if tc.envName != "" {
				text = "log: {level: info, format: json}"
				t.Setenv(tc.envName, tc.envValue)
			}
			_, err := Load(writeYAML(t, text))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "private-value") {
				t.Fatal("error exposes input value")
			}
		})
	}
}

func TestMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yml"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected wrapped file error, got %v", err)
	}
}

func TestPostgresENV(t *testing.T) {
	setupConfigENV(t)
	t.Setenv("POSTGRES_PORT", "55434")
	t.Setenv("POSTGRES_MAX_CONNS", "3")
	t.Setenv("POSTGRES_TIMEOUT", "2s")
	cfg, err := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.Port != 55434 || cfg.Postgres.MaxConns != 3 || cfg.Postgres.Timeout.String() != "2s" || cfg.Postgres.Password != "test-password" {
		t.Fatal("PostgreSQL ENV overrides not applied")
	}
}

func TestInvalidPostgresENV(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"POSTGRES_PORT", "65536"}, {"POSTGRES_PORT", "0"}, {"POSTGRES_TIMEOUT", "secret-value"},
		{"POSTGRES_TIMEOUT", "-1s"}, {"POSTGRES_MAX_CONNS", "0"}, {"POSTGRES_PASSWORD", ""},
	} {
		t.Run(tc.name+"/"+tc.value, func(t *testing.T) {
			setupConfigENV(t)
			t.Setenv(tc.name, tc.value)
			_, err := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
			if err == nil {
				t.Fatal("expected invalid configuration")
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error discloses input")
			}
		})
	}
}

func TestPostgresPasswordInYAML(t *testing.T) {
	setupConfigENV(t)
	text := "log: {level: info, format: json}\npostgres: {password: secret-value}"
	_, err := Load(writeYAML(t, text))
	if err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatal("YAML password must be rejected safely")
	}
}

func TestMinIOENV(t *testing.T) {
	setupConfigENV(t)
	t.Setenv("MINIO_ENDPOINT", "localhost:59000")
	t.Setenv("MINIO_BUCKET", "test-images")
	t.Setenv("MINIO_SECURE", "true")
	t.Setenv("MINIO_TIMEOUT", "2s")
	c, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
	if e != nil {
		t.Fatal(e)
	}
	if c.MinIO.Endpoint != "localhost:59000" || c.MinIO.Bucket != "test-images" || !c.MinIO.Secure || c.MinIO.Timeout.String() != "2s" || c.MinIO.SecretKey != "test-secret-key" {
		t.Fatal("MinIO ENV not applied")
	}
}
func TestMinIOInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ name, value string }{{"MINIO_ENDPOINT", "http://secret-value@host"}, {"MINIO_BUCKET", "Invalid_Bucket"}, {"MINIO_SECURE", "secret-value"}, {"MINIO_SECRET_KEY", ""}, {"MINIO_TIMEOUT", "0s"}} {
		t.Run(tc.name, func(t *testing.T) {
			setupConfigENV(t)
			t.Setenv(tc.name, tc.value)
			_, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
			if e == nil || strings.Contains(e.Error(), "secret-value") {
				t.Fatal("unsafe or missing validation error", e)
			}
		})
	}
	setupConfigENV(t)
	if _, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+"\nminio: {secret_key: private-value}")); e == nil || strings.Contains(e.Error(), "private-value") {
		t.Fatal("YAML secret accepted or exposed")
	}
}

func TestKafkaENV(t *testing.T) {
	setupConfigENV(t)
	t.Setenv("KAFKA_BROKERS", "localhost:59092,localhost:59093")
	t.Setenv("KAFKA_JOBS_TOPIC", "test.jobs")
	t.Setenv("KAFKA_DLQ_TOPIC", "test.dlq")
	t.Setenv("KAFKA_CONSUMER_GROUP", "test-group")
	t.Setenv("KAFKA_TIMEOUT", "2s")
	c, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
	if e != nil {
		t.Fatal(e)
	}
	if len(c.Kafka.Brokers) != 2 || c.Kafka.Brokers[0] != "localhost:59092" || c.Kafka.JobsTopic != "test.jobs" || c.Kafka.DLQTopic != "test.dlq" || c.Kafka.ConsumerGroup != "test-group" || c.Kafka.Timeout.String() != "2s" {
		t.Fatal("Kafka overrides not applied")
	}
}
func TestKafkaInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ name, value string }{{"KAFKA_BROKERS", "localhost"}, {"KAFKA_BROKERS", "localhost:0"}, {"KAFKA_BROKERS", "localhost:65536"}, {"KAFKA_JOBS_TOPIC", ".."}, {"KAFKA_DLQ_TOPIC", "image.processing.jobs"}, {"KAFKA_CONSUMER_GROUP", " "}, {"KAFKA_TIMEOUT", "private-value"}, {"KAFKA_TIMEOUT", "0s"}, {"KAFKA_BROKERS", ""}} {
		t.Run(tc.name+"/"+tc.value, func(t *testing.T) {
			setupConfigENV(t)
			t.Setenv(tc.name, tc.value)
			_, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
			if e == nil || strings.Contains(e.Error(), "private-value") {
				t.Fatal("missing or unsafe validation", e)
			}
		})
	}
}

func TestApplicationSettings(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{{"HTTP_ADDRESS", "127.0.0.1:8088", true}, {"HTTP_ADDRESS", "bad", false}, {"HTTP_SHUTDOWN_TIMEOUT", "31s", false}, {"HTTP_READ_TIMEOUT", "0s", false}, {"STARTUP_TIMEOUT", "0s", false}, {"VALIDATION_CONCURRENCY", "0", false}, {"MIGRATIONS_DIR", "", false}} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			setupConfigENV(t)
			t.Setenv(tc.name, tc.value)
			_, e := Load(writeYAML(t, "log: {level: info, format: json}"+postgresYAML+minioYAML+kafkaYAML))
			if (e == nil) != tc.valid {
				t.Fatal(e)
			}
		})
	}
}
