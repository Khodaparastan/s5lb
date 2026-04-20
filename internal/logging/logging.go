// Package logging configures the process-wide slog handler.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// ParseLevel maps a human string to slog.Level. Unknown -> Info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New builds a structured logger with the given level + format (json|text).
// Attaches service metadata via .With().
func New(level slog.Level, format, service, version string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if strings.ToLower(format) == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", service, "version", version)
}
