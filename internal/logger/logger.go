package logger

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/akhmed9505/image-processor/internal/config"
)

// New creates a logger writing to output without changing the default logger.
// The caller owns output and is responsible for closing it when necessary.
func New(cfg config.Log, output io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("logger: unsupported level %q", cfg.Level)
	}

	options := &slog.HandlerOptions{Level: level}
	switch cfg.Format {
	case "text":
		return slog.New(slog.NewTextHandler(output, options)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(output, options)), nil
	default:
		return nil, fmt.Errorf("logger: unsupported format %q", cfg.Format)
	}
}
