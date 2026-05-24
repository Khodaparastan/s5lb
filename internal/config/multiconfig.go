package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/khodaparastan/socks5lb/internal/logging"
)

// optionalDuration is a time.Duration that can be absent in YAML.
// A zero value means "not set — inherit from global config".
type optionalDuration struct {
	d   time.Duration
	set bool
}

func (o *optionalDuration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	o.d = d
	o.set = true
	return nil
}

// MarshalYAML implements yaml.Marshaler so the field round-trips correctly.
func (o optionalDuration) MarshalYAML() (interface{}, error) {
	if !o.set {
		return nil, nil
	}
	return o.d.String(), nil
}

// GroupConfig holds data-plane settings for a single upstream group.
// Every field that is zero/empty inherits from the global Config defaults.
// Global-only fields (OTel, AdminAddr, LogLevel, LogFormat, AdminToken,
// AdminPprof) are intentionally absent — set them at the top level.
type GroupConfig struct {
	// Name uniquely identifies this group. Required when groups are defined.
	Name string `yaml:"name"`

	// Listen is the SOCKS5 frontend address for this group.
	// If empty, falls back to the global listen address.
	Listen string `yaml:"listen,omitempty"`

	MaxPerProxy *int `yaml:"max_per_proxy,omitempty"`
	MaxClients  *int `yaml:"max_clients,omitempty"`

	HealthInterval   optionalDuration `yaml:"health_interval,omitempty"`
	RetryBackoff     optionalDuration `yaml:"retry_backoff,omitempty"`
	ConnectTimeout   optionalDuration `yaml:"connect_timeout,omitempty"`
	HandshakeTimeout optionalDuration `yaml:"handshake_timeout,omitempty"`
	QueueWaitTimeout optionalDuration `yaml:"queue_wait_timeout,omitempty"`

	FailureThreshold *int             `yaml:"failure_threshold,omitempty"`
	FailureWindow    optionalDuration `yaml:"failure_window,omitempty"`

	IdleTimeout optionalDuration `yaml:"idle_timeout,omitempty"`

	DrainSoftTimeout optionalDuration `yaml:"drain_soft_timeout,omitempty"`
	DrainHardTimeout optionalDuration `yaml:"drain_hard_timeout,omitempty"`

	TCPKeepAlive *bool `yaml:"tcp_keepalive,omitempty"`

	Strategy string  `yaml:"strategy,omitempty"`
	HashKey  HashKey `yaml:"hash_key,omitempty"`

	Backpressure         BackpressureStrategy `yaml:"backpressure,omitempty"`
	AdmissionWaitTimeout optionalDuration     `yaml:"admission_wait_timeout,omitempty"`

	UDPEnabled     *bool            `yaml:"udp_enabled,omitempty"`
	UDPBindAddr    string           `yaml:"udp_bind,omitempty"`
	UDPIdleTimeout optionalDuration `yaml:"udp_idle_timeout,omitempty"`

	// Transport allows a per-group transport override.
	Transport *TransportConfig `yaml:"transport,omitempty"`

	Upstreams []UpstreamSpec `yaml:"upstreams"`
}

// MultiConfig is the top-level config structure supporting multiple upstream groups.
// It embeds Config inline for full backward compatibility with single-group configs.
// When Groups is non-empty, each group defines its own upstream pool; group entries
// override global data-plane defaults field by field (nil/zero means "use global").
type MultiConfig struct {
	Config `yaml:",inline"`

	// Groups defines named upstream pools. When empty, the embedded Config
	// is used as a single unnamed "default" group (backward compatible).
	Groups []GroupConfig `yaml:"groups"`
}

// LoadMultiFile reads and parses a YAML config file into a MultiConfig.
// It starts from Defaults() so all missing fields are filled with sensible values.
func LoadMultiFile(path string) (MultiConfig, error) {
	mc := MultiConfig{Config: Defaults()}
	raw, err := os.ReadFile(path)
	if err != nil {
		return mc, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, &mc); err != nil {
		return mc, fmt.Errorf("parse config %s: %w", path, err)
	}
	mc.Config.LogLevel = logging.ParseLevel(mc.Config.LogLevelS)
	if mc.Config.OTel.ServiceName == "" {
		mc.Config.OTel.ServiceName = "socks5lb"
	}
	return mc, nil
}

// EffectiveGroups returns one Config per group, merging group-level overrides
// onto the global defaults. When no groups are defined, a single effective
// group is returned using the embedded Config unchanged (single-group compat).
// The GroupName field in each returned Config is set to the group's Name.
func (mc *MultiConfig) EffectiveGroups() []Config {
	if len(mc.Groups) == 0 {
		cfg := mc.Config
		if cfg.GroupName == "" {
			cfg.GroupName = "default"
		}
		return []Config{cfg}
	}

	out := make([]Config, 0, len(mc.Groups))
	for _, g := range mc.Groups {
		eff := mc.Config // copy global defaults

		if g.Listen != "" {
			eff.ListenAddr = g.Listen
		}
		if g.MaxPerProxy != nil {
			eff.MaxPerProxy = *g.MaxPerProxy
		}
		if g.MaxClients != nil {
			eff.MaxClients = *g.MaxClients
		}
		if g.HealthInterval.set {
			eff.HealthInterval = g.HealthInterval.d
		}
		if g.RetryBackoff.set {
			eff.RetryBackoff = g.RetryBackoff.d
		}
		if g.ConnectTimeout.set {
			eff.ConnectTimeout = g.ConnectTimeout.d
		}
		if g.HandshakeTimeout.set {
			eff.HandshakeTimeout = g.HandshakeTimeout.d
		}
		if g.QueueWaitTimeout.set {
			eff.QueueWaitTimeout = g.QueueWaitTimeout.d
		}
		if g.FailureThreshold != nil {
			eff.FailureThreshold = *g.FailureThreshold
		}
		if g.FailureWindow.set {
			eff.FailureWindow = g.FailureWindow.d
		}
		if g.IdleTimeout.set {
			eff.IdleTimeout = g.IdleTimeout.d
		}
		if g.DrainSoftTimeout.set {
			eff.DrainSoftTimeout = g.DrainSoftTimeout.d
		}
		if g.DrainHardTimeout.set {
			eff.DrainHardTimeout = g.DrainHardTimeout.d
		}
		if g.TCPKeepAlive != nil {
			eff.TCPKeepAlive = *g.TCPKeepAlive
		}
		if g.Strategy != "" {
			eff.Strategy = g.Strategy
		}
		if g.HashKey != "" {
			eff.HashKey = g.HashKey
		}
		if g.Backpressure != "" {
			eff.Backpressure = g.Backpressure
		}
		if g.AdmissionWaitTimeout.set {
			eff.AdmissionWaitTimeout = g.AdmissionWaitTimeout.d
		}
		if g.UDPEnabled != nil {
			eff.UDPEnabled = *g.UDPEnabled
		}
		if g.UDPBindAddr != "" {
			eff.UDPBindAddr = g.UDPBindAddr
		}
		if g.UDPIdleTimeout.set {
			eff.UDPIdleTimeout = g.UDPIdleTimeout.d
		}
		if g.Transport != nil {
			eff.Transport = *g.Transport
		}
		if len(g.Upstreams) > 0 {
			eff.Upstreams = g.Upstreams
		}

		eff.GroupName = g.Name
		out = append(out, eff)
	}
	return out
}

// Validate validates the MultiConfig. In single-group mode it validates the
// embedded Config. In multi-group mode it validates group names for uniqueness
// and then validates each effective group's Config.
func (mc *MultiConfig) Validate() error {
	if len(mc.Groups) == 0 {
		return mc.Config.Validate()
	}

	seen := make(map[string]struct{}, len(mc.Groups))
	for i, g := range mc.Groups {
		if g.Name == "" {
			return fmt.Errorf("groups[%d]: name is required", i)
		}
		if _, dup := seen[g.Name]; dup {
			return fmt.Errorf("groups[%d]: duplicate group name %q", i, g.Name)
		}
		seen[g.Name] = struct{}{}
	}

	effs := mc.EffectiveGroups()
	listenAddrs := make(map[string]string, len(effs)) // addr -> first group name
	for i, eff := range effs {
		if err := eff.Validate(); err != nil {
			return fmt.Errorf("group %q: %w", mc.Groups[i].Name, err)
		}
		if first, dup := listenAddrs[eff.ListenAddr]; dup {
			return fmt.Errorf("group %q: listen addr %q already used by group %q",
				mc.Groups[i].Name, eff.ListenAddr, first)
		}
		listenAddrs[eff.ListenAddr] = mc.Groups[i].Name
	}
	return nil
}

// GroupNames returns the ordered list of group names.
// In single-group mode returns ["default"] (or the GroupName if set).
func (mc *MultiConfig) GroupNames() []string {
	if len(mc.Groups) == 0 {
		name := mc.Config.GroupName
		if name == "" {
			name = "default"
		}
		return []string{name}
	}
	names := make([]string, len(mc.Groups))
	for i, g := range mc.Groups {
		names[i] = g.Name
	}
	return names
}
