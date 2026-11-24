package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func clearLogENV(t *testing.T) {
	t.Helper()
	for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
}

func writeYAML(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

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
			clearLogENV(t)
			for name, value := range tc.overrides {
				t.Setenv(name, value)
			}
			cfg, err := Load(writeYAML(t, tc.yaml))
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
			clearLogENV(t)
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
