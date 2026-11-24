package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"go.yaml.in/yaml/v3"
)

// Config contains the application settings currently in use.
type Config struct {
	Kafka    Kafka    `yaml:"kafka"`
	MinIO    MinIO    `yaml:"minio"`
	Log      Log      `yaml:"log"`
	Postgres Postgres `yaml:"postgres"`
}

// Log configures the logging threshold and output format.
type Log struct {
	Level  string `yaml:"level" env:"LOG_LEVEL"`
	Format string `yaml:"format" env:"LOG_FORMAT"`
}

// Postgres configures the local PostgreSQL connection and pool.
// User and Password are accepted only through ENV.
type Postgres struct {
	Host     string        `yaml:"host" env:"POSTGRES_HOST"`
	Port     uint16        `yaml:"port" env:"POSTGRES_PORT"`
	Database string        `yaml:"database" env:"POSTGRES_DB"`
	User     string        `yaml:"-" env:"POSTGRES_USER"`
	Password string        `yaml:"-" env:"POSTGRES_PASSWORD"`
	MaxConns int32         `yaml:"max_conns" env:"POSTGRES_MAX_CONNS"`
	Timeout  time.Duration `yaml:"timeout" env:"POSTGRES_TIMEOUT"`
}

// MinIO configures object storage; credentials are accepted only through ENV.
type MinIO struct {
	Endpoint  string        `yaml:"endpoint" env:"MINIO_ENDPOINT"`
	Bucket    string        `yaml:"bucket" env:"MINIO_BUCKET"`
	Secure    bool          `yaml:"secure" env:"MINIO_SECURE"`
	AccessKey string        `yaml:"-" env:"MINIO_ACCESS_KEY"`
	SecretKey string        `yaml:"-" env:"MINIO_SECRET_KEY"`
	Timeout   time.Duration `yaml:"timeout" env:"MINIO_TIMEOUT"`
}

// Validate checks the endpoint, bucket, credentials, and initialization timeout.
func (c MinIO) Validate() error {
	u, e := url.Parse("http://" + c.Endpoint)
	if e != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("minio.endpoint must be a host with optional port")
	}
	valid := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	if !valid.MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") || strings.Contains(c.Bucket, ".-") || strings.Contains(c.Bucket, "-.") || net.ParseIP(c.Bucket) != nil {
		return errors.New("minio.bucket must be a valid S3 bucket name")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("minio ENV credentials are required")
	}
	if c.Timeout <= 0 {
		return errors.New("minio.timeout must be positive")
	}
	return nil
}

// Kafka configures local plaintext brokers, topics, and the worker group.
type Kafka struct {
	Brokers       []string      `yaml:"brokers" env:"KAFKA_BROKERS"`
	JobsTopic     string        `yaml:"jobs_topic" env:"KAFKA_JOBS_TOPIC"`
	DLQTopic      string        `yaml:"dlq_topic" env:"KAFKA_DLQ_TOPIC"`
	ConsumerGroup string        `yaml:"consumer_group" env:"KAFKA_CONSUMER_GROUP"`
	Timeout       time.Duration `yaml:"timeout" env:"KAFKA_TIMEOUT"`
}

// Validate checks broker addresses, topic names, group, and connection timeout.
func (c Kafka) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka.brokers is required")
	}
	for _, broker := range c.Brokers {
		host, port, e := net.SplitHostPort(broker)
		n, pe := strconv.ParseUint(port, 10, 16)
		if e != nil || host == "" || strings.TrimSpace(host) != host || pe != nil || n == 0 {
			return errors.New("kafka.brokers must contain host:port addresses")
		}
	}
	valid := regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
	for _, topic := range []string{c.JobsTopic, c.DLQTopic} {
		if !valid.MatchString(topic) || topic == "." || topic == ".." {
			return errors.New("kafka topic names are invalid")
		}
	}
	if c.JobsTopic == c.DLQTopic {
		return errors.New("kafka topics must be distinct")
	}
	if strings.TrimSpace(c.ConsumerGroup) == "" {
		return errors.New("kafka.consumer_group is required")
	}
	if c.Timeout < time.Millisecond {
		return errors.New("kafka.timeout must be at least one millisecond")
	}
	return nil
}

// Load reads one strict YAML document, applies ENV overrides, and validates it.
// Unset ENV variables preserve YAML values; explicitly empty overrides are invalid.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open configuration: %w", err)
	}
	defer f.Close()
	var cfg Config
	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, errors.New("parse configuration: expected valid YAML with known fields and correctly typed values")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("parse configuration: expected exactly one YAML document")
	}
	// env skips empty strings, so reject them rather than silently retaining YAML.
	for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_MAX_CONNS", "POSTGRES_TIMEOUT", "MINIO_ENDPOINT", "MINIO_BUCKET", "MINIO_SECURE", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_TIMEOUT", "KAFKA_BROKERS", "KAFKA_JOBS_TOPIC", "KAFKA_DLQ_TOPIC", "KAFKA_CONSUMER_GROUP", "KAFKA_TIMEOUT"} {
		if value, set := os.LookupEnv(name); set && value == "" {
			return Config{}, fmt.Errorf("environment configuration: %s must not be empty", name)
		}
	}
	if err := env.Parse(&cfg); err != nil {
		return Config{}, errors.New("parse environment configuration: invalid ENV value; check variable types")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate configuration: %w", err)
	}
	return cfg, nil
}

// Validate checks required application settings.
func (c Config) Validate() error {
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("log.level must be debug, info, warn or error")
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return errors.New("log.format must be json or text")
	}
	if strings.TrimSpace(c.Postgres.Host) == "" || c.Postgres.Port == 0 {
		return errors.New("postgres host and nonzero port are required")
	}
	if c.Postgres.Database == "" || c.Postgres.User == "" || c.Postgres.Password == "" {
		return errors.New("postgres database, user and ENV password are required")
	}
	if c.Postgres.MaxConns <= 0 {
		return errors.New("postgres.max_conns must be positive")
	}
	if c.Postgres.Timeout <= 0 {
		return errors.New("postgres.timeout must be positive")
	}
	if err := c.MinIO.Validate(); err != nil {
		return err
	}
	return c.Kafka.Validate()
}
