package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadFile reads and parses a YAML config file into a Config, starting from
// Defaults(). Missing file is returned as an error; use os.IsNotExist to detect.
func LoadFile(path string) (Config, error) {
	cfg := Defaults()
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.LogLevel = parseLevel(cfg.LogLevelS)
	if cfg.OTel.ServiceName == "" {
		cfg.OTel.ServiceName = "socks5lb"
	}
	if cfg.OTel.SampleRatio == 0 {
		cfg.OTel.SampleRatio = 1.0
	}
	return cfg, nil
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "", "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
