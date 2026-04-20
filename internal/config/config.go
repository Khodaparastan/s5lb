// Package config holds the runtime configuration, YAML loader, and upstream
// specification parser.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// BackpressureStrategy selects admission behavior when MaxClients is saturated.
type BackpressureStrategy string

const (
	BackpressureReject             BackpressureStrategy = "reject"
	BackpressureWait               BackpressureStrategy = "wait"
	BackpressureDropOldest         BackpressureStrategy = "drop-oldest"
	BackpressureDropLowestPriority BackpressureStrategy = "drop-lowest-priority"
)

// HashKey selects which attribute feeds consistent-hash strategies.
type HashKey string

const (
	HashClientIP    HashKey = "client-ip"
	HashDestination HashKey = "destination"
	HashDestHost    HashKey = "destination-host"
)

// Config is the top-level runtime configuration.
type Config struct {
	ListenAddr string `yaml:"listen"`
	AdminAddr  string `yaml:"admin"`

	MaxPerProxy int `yaml:"max_per_proxy"`
	MaxClients  int `yaml:"max_clients"`

	HealthInterval   time.Duration `yaml:"health_interval"`
	RetryBackoff     time.Duration `yaml:"retry_backoff"`
	ConnectTimeout   time.Duration `yaml:"connect_timeout"`
	HandshakeTimeout time.Duration `yaml:"handshake_timeout"`
	QueueWaitTimeout time.Duration `yaml:"queue_wait_timeout"`

	FailureThreshold int           `yaml:"failure_threshold"`
	FailureWindow    time.Duration `yaml:"failure_window"`

	IdleTimeout time.Duration `yaml:"idle_timeout"`

	// Two-phase drain.
	DrainSoftTimeout time.Duration `yaml:"drain_soft_timeout"`
	DrainHardTimeout time.Duration `yaml:"drain_hard_timeout"`

	TCPKeepAlive bool `yaml:"tcp_keepalive"`

	Strategy string  `yaml:"strategy"`
	HashKey  HashKey `yaml:"hash_key"`

	// Backpressure.
	Backpressure         BackpressureStrategy `yaml:"backpressure"`
	AdmissionWaitTimeout time.Duration        `yaml:"admission_wait_timeout"`

	// UDP ASSOCIATE.
	UDPEnabled     bool          `yaml:"udp_enabled"`
	UDPBindAddr    string        `yaml:"udp_bind"` // interface to bind the client-facing UDP socket; empty = same host as listen
	UDPIdleTimeout time.Duration `yaml:"udp_idle_timeout"`

	// OpenTelemetry.
	OTel OTelConfig `yaml:"otel"`

	// Logging.
	LogLevel  slog.Level `yaml:"-"`
	LogLevelS string     `yaml:"log_level"`
	LogFormat string     `yaml:"log_format"`

	// Upstreams (from file). Flag-specified upstreams are merged on top.
	Upstreams []UpstreamSpec `yaml:"upstreams"`
}

// UpstreamSpec is the file-side form of an upstream definition.
type UpstreamSpec struct {
	ID       string `yaml:"id"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Weight   int    `yaml:"weight"`
	Priority int    `yaml:"priority"`
}

// OTelConfig controls tracing export.
type OTelConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Endpoint    string            `yaml:"endpoint"`     // e.g., "otel-collector:4317"
	Insecure    bool              `yaml:"insecure"`     // true to disable TLS
	ServiceName string            `yaml:"service_name"` // default "socks5lb"
	SampleRatio float64           `yaml:"sample_ratio"` // 0.0–1.0, default 1.0
	Headers     map[string]string `yaml:"headers"`
}

// Defaults returns a baseline config.
func Defaults() Config {
	return Config{
		ListenAddr:           "127.0.0.1:1080",
		AdminAddr:            "127.0.0.1:9090",
		MaxPerProxy:          100,
		MaxClients:           4096,
		HealthInterval:       20 * time.Second,
		RetryBackoff:         30 * time.Second,
		ConnectTimeout:       5 * time.Second,
		HandshakeTimeout:     10 * time.Second,
		QueueWaitTimeout:     10 * time.Second,
		FailureThreshold:     5,
		FailureWindow:        30 * time.Second,
		IdleTimeout:          0,
		DrainSoftTimeout:     20 * time.Second,
		DrainHardTimeout:     10 * time.Second,
		TCPKeepAlive:         true,
		Strategy:             "least-active",
		HashKey:              HashClientIP,
		Backpressure:         BackpressureReject,
		AdmissionWaitTimeout: 2 * time.Second,
		UDPEnabled:           true,
		UDPBindAddr:          "",
		UDPIdleTimeout:       60 * time.Second,
		OTel: OTelConfig{
			Enabled:     false,
			ServiceName: "socks5lb",
			SampleRatio: 1.0,
		},
		LogLevel:  slog.LevelInfo,
		LogFormat: "json",
	}
}

// Validate sanity-checks a Config. Call after all merges.
func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		return errors.New("listen addr is required")
	}
	if c.MaxPerProxy <= 0 {
		return errors.New("max_per_proxy must be > 0")
	}
	if c.MaxClients <= 0 {
		return errors.New("max_clients must be > 0")
	}
	if c.ConnectTimeout <= 0 || c.HandshakeTimeout <= 0 {
		return errors.New("connect/handshake timeouts must be > 0")
	}
	if c.DrainSoftTimeout < 0 || c.DrainHardTimeout < 0 {
		return errors.New("drain timeouts must be >= 0")
	}
	switch c.Backpressure {
	case BackpressureReject, BackpressureWait, BackpressureDropOldest, BackpressureDropLowestPriority:
	default:
		return fmt.Errorf("unknown backpressure strategy %q", c.Backpressure)
	}
	switch c.HashKey {
	case HashClientIP, HashDestination, HashDestHost:
	default:
		return fmt.Errorf("unknown hash_key %q", c.HashKey)
	}
	if c.OTel.Enabled && c.OTel.Endpoint == "" {
		return errors.New("otel.enabled=true requires otel.endpoint")
	}
	if c.OTel.SampleRatio < 0 || c.OTel.SampleRatio > 1 {
		return errors.New("otel.sample_ratio must be in [0,1]")
	}
	return nil
}

// BuildUpstreams converts file-specified UpstreamSpecs into Upstream objects.
func (c *Config) BuildUpstreams() ([]*upstream.Upstream, error) {
	out := make([]*upstream.Upstream, 0, len(c.Upstreams))
	for i, s := range c.Upstreams {
		if s.Host == "" || s.Port <= 0 {
			return nil, fmt.Errorf("upstream[%d]: host and port required", i)
		}
		w := s.Weight
		if w <= 0 {
			w = 1
		}
		u := upstream.New(s.Host, s.Port, s.Username, s.Password, w, s.Priority)
		if s.ID != "" {
			u.ID = s.ID
		}
		out = append(out, u)
	}
	return out, nil
}
