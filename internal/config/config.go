// Package config holds the runtime configuration, YAML loader, and upstream
// specification parser.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
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

// TransportType selects how upstream byte streams are opened.
type TransportType string

const (
	TransportDirect         TransportType = "direct"
	TransportKubernetesExec TransportType = "kubernetes-exec"
)

// TransportConfig controls upstream transport behavior.
type TransportConfig struct {
	Type TransportType `yaml:"type"`

	Kubernetes KubernetesExecTransportConfig `yaml:"kubernetes"`
}

// KubernetesExecTransportConfig controls Kubernetes exec upstream transport.
type KubernetesExecTransportConfig struct {
	Kubeconfig string `yaml:"kubeconfig"`
	Context    string `yaml:"context"`

	Namespace string `yaml:"namespace"`
	Pod       string `yaml:"pod"`
	Container string `yaml:"container"`

	// spdy, websocket, websocket-preferred, raw-wss
	Mode string `yaml:"mode"`

	// Command template.
	//
	// Supported placeholders:
	//   {{host}}
	//   {{port}}
	//   {{address}}
	//
	// Recommended:
	//   ["/usr/local/bin/kube-socks-relay", "tcp", "{{host}}", "{{port}}"]
	Command []string `yaml:"command"`

	// WSSURL is a direct Kubernetes pod exec WebSocket URL.
	//
	// Example:
	//   wss://api.example.com/api/v1/namespaces/ns/pods/pod/exec?container=relay
	//
	// When WSSURL is set, raw WebSocket exec is used.
	WSSURL string `yaml:"wss_url"`

	BearerToken     string `yaml:"bearer_token"`
	BearerTokenFile string `yaml:"bearer_token_file"`

	CAFile                string `yaml:"ca_file"`
	InsecureSkipTLSVerify bool   `yaml:"insecure_skip_tls_verify"`
	ServerName            string `yaml:"server_name"`

	Headers map[string]string `yaml:"headers"`
}

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

	DrainSoftTimeout time.Duration `yaml:"drain_soft_timeout"`
	DrainHardTimeout time.Duration `yaml:"drain_hard_timeout"`

	TCPKeepAlive bool `yaml:"tcp_keepalive"`

	Strategy string  `yaml:"strategy"`
	HashKey  HashKey `yaml:"hash_key"`

	Backpressure         BackpressureStrategy `yaml:"backpressure"`
	AdmissionWaitTimeout time.Duration        `yaml:"admission_wait_timeout"`

	UDPEnabled       bool          `yaml:"udp_enabled"`
	UDPBindAddr      string        `yaml:"udp_bind"`
	UDPAdvertiseAddr string        `yaml:"udp_advertise"`
	UDPIdleTimeout   time.Duration `yaml:"udp_idle_timeout"`

	// Upstream transport.
	Transport TransportConfig `yaml:"transport"`

	OTel OTelConfig `yaml:"otel"`

	LogLevel  slog.Level `yaml:"-"`
	LogLevelS string     `yaml:"log_level"`
	LogFormat string     `yaml:"log_format"`

	// Admin authentication and profiling.
	AdminToken string `yaml:"admin_token"`
	AdminPprof bool   `yaml:"admin_pprof"`

	Upstreams []UpstreamSpec `yaml:"upstreams"`

	// GroupName is set programmatically (not from YAML) to identify the owning group.
	GroupName string `yaml:"-"`
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
	Endpoint    string            `yaml:"endpoint"`
	Insecure    bool              `yaml:"insecure"`
	ServiceName string            `yaml:"service_name"`
	SampleRatio float64           `yaml:"sample_ratio"`
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
		Transport: TransportConfig{
			Type: TransportDirect,
		},
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
	if c.HealthInterval <= 0 {
		return errors.New("health_interval must be > 0")
	}
	if c.RetryBackoff < 0 {
		return errors.New("retry_backoff must be >= 0")
	}
	if c.ConnectTimeout <= 0 || c.HandshakeTimeout <= 0 {
		return errors.New("connect/handshake timeouts must be > 0")
	}
	if c.QueueWaitTimeout < 0 {
		return errors.New("queue_wait_timeout must be >= 0")
	}
	if c.AdmissionWaitTimeout < 0 {
		return errors.New("admission_wait_timeout must be >= 0 (zero means unbounded wait)")
	}
	if c.FailureThreshold <= 0 {
		return errors.New("failure_threshold must be > 0")
	}
	if c.FailureWindow <= 0 {
		return errors.New("failure_window must be > 0")
	}
	if c.IdleTimeout < 0 {
		return errors.New("idle_timeout must be >= 0")
	}
	if c.DrainSoftTimeout < 0 || c.DrainHardTimeout < 0 {
		return errors.New("drain timeouts must be >= 0")
	}
	if c.UDPEnabled && c.UDPIdleTimeout <= 0 {
		return errors.New("udp_idle_timeout must be > 0 when UDP is enabled")
	}
	if c.UDPAdvertiseAddr != "" && net.ParseIP(c.UDPAdvertiseAddr) == nil {
		return fmt.Errorf("udp_advertise must be an IP address, got %q", c.UDPAdvertiseAddr)
	}

	if len(c.Upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}
	seenIDs := make(map[string]struct{}, len(c.Upstreams))
	for i, u := range c.Upstreams {
		if u.Host == "" {
			return fmt.Errorf("upstream[%d].host is required", i)
		}
		if u.Port < 1 || u.Port > 65535 {
			return fmt.Errorf("upstream[%d].port must be in [1,65535]", i)
		}
		if u.Weight < 0 {
			return fmt.Errorf("upstream[%d].weight must be >= 0", i)
		}
		if u.ID != "" {
			if _, dup := seenIDs[u.ID]; dup {
				return fmt.Errorf("upstream[%d]: duplicate upstream ID %q", i, u.ID)
			}
			seenIDs[u.ID] = struct{}{}
		}
	}

	if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
		return fmt.Errorf("listen addr %q is invalid: %w", c.ListenAddr, err)
	}

	if err := validateStrategyName(c.Strategy); err != nil {
		return err
	}

	switch c.Backpressure {
	case BackpressureReject,
		BackpressureWait,
		BackpressureDropOldest,
		BackpressureDropLowestPriority:
	default:
		return fmt.Errorf("unknown backpressure strategy %q", c.Backpressure)
	}

	switch c.HashKey {
	case HashClientIP, HashDestination, HashDestHost:
	default:
		return fmt.Errorf("unknown hash_key %q", c.HashKey)
	}

	switch c.Transport.Type {
	case "", TransportDirect:
		c.Transport.Type = TransportDirect

	case TransportKubernetesExec:
		k := c.Transport.Kubernetes

		if k.Mode == "" {
			k.Mode = "websocket-preferred"
			c.Transport.Kubernetes = k
		}

		if k.WSSURL != "" || k.Mode == "raw-wss" {
			if k.WSSURL == "" {
				return errors.New("transport.kubernetes.wss_url is required when mode=raw-wss")
			}
		} else {
			if k.Pod == "" {
				return errors.New("transport.kubernetes.pod is required for kubernetes-exec")
			}
			if k.Namespace == "" {
				return errors.New("transport.kubernetes.namespace is required for kubernetes-exec")
			}
		}

		if len(k.Command) == 0 {
			return errors.New("transport.kubernetes.command is required for kubernetes-exec")
		}

	default:
		return fmt.Errorf("unknown transport.type %q", c.Transport.Type)
	}

	if c.OTel.Enabled && c.OTel.Endpoint == "" {
		return errors.New("otel.enabled=true requires otel.endpoint")
	}
	if c.OTel.SampleRatio < 0 || c.OTel.SampleRatio > 1 {
		return errors.New("otel.sample_ratio must be in [0,1]")
	}

	return nil
}

// validStrategyNames is the set of strategy names accepted by config validation.
// This list mirrors strategy/registry.go and must be kept in sync.
var validStrategyNames = map[string]bool{
	"":                     true, // default (least-active)
	"least-active":         true,
	"leastconn":            true,
	"round-robin":          true,
	"rr":                   true,
	"weighted-round-robin": true,
	"wrr":                  true,
	"random":               true,
	"weighted-random":      true,
	"wrandom":              true,
	"p2c":                  true,
	"power-of-two":         true,
	"least-latency":        true,
	"ll":                   true,
	"consistent-hash":      true,
	"hash":                 true,
	"priority-failover":    true,
	"priority":             true,
	"failover":             true,
}

func validateStrategyName(name string) error {
	if !validStrategyNames[strings.ToLower(strings.TrimSpace(name))] {
		return fmt.Errorf("unknown strategy %q", name)
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
