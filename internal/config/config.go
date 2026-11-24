package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/caarlos0/env/v11"
	"go.yaml.in/yaml/v3"
)

// Config contains the application settings currently in use.
type Config struct {
	Log Log `yaml:"log"`
}

// Log configures the logging threshold and output format.
type Log struct {
	Level  string `yaml:"level" env:"LOG_LEVEL"`
	Format string `yaml:"format" env:"LOG_FORMAT"`
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
	for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT"} {
		if value, set := os.LookupEnv(name); set && value == "" {
			return Config{}, fmt.Errorf("environment configuration: %s must not be empty", name)
		}
	}
	if err := env.Parse(&cfg); err != nil {
		return Config{}, errors.New("parse environment configuration: invalid logging settings")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate configuration: %w", err)
	}
	return cfg, nil
}

// Validate checks the supported logging levels and formats.
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
	return nil
}
