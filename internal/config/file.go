package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/khodaparastan/socks5lb/internal/logging"
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
	cfg.LogLevel = logging.ParseLevel(cfg.LogLevelS)
	if cfg.OTel.ServiceName == "" {
		cfg.OTel.ServiceName = "socks5lb"
	}
	return cfg, nil
}
