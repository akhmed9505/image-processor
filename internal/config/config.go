package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"go.yaml.in/yaml/v3"
)

// Config contains the application settings currently in use.
type Config struct {
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
	for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_MAX_CONNS", "POSTGRES_TIMEOUT"} {
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

// Validate checks logging settings and required PostgreSQL connection parameters.
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
	return nil
}
