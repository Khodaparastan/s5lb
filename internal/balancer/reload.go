package balancer

import (
	"fmt"
	"log/slog"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/transport"
)

// reloadFromConfig applies an already-parsed Config snapshot to the LoadBalancer.
// It is used by Manager.Reload so the file is not re-read redundantly.
func (lb *LoadBalancer) reloadFromConfig(newCfg config.Config) error {
	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	oldCfg := lb.Config()

	warnImmutable(lb.log, "listen", oldCfg.ListenAddr, newCfg.ListenAddr)
	warnImmutable(lb.log, "admin", oldCfg.AdminAddr, newCfg.AdminAddr)
	warnImmutable(lb.log, "strategy", oldCfg.Strategy, newCfg.Strategy)
	warnImmutable(lb.log, "hash_key", string(oldCfg.HashKey), string(newCfg.HashKey))
	warnImmutableInt(lb.log, "max_clients", oldCfg.MaxClients, newCfg.MaxClients)

	newCfg.ListenAddr = oldCfg.ListenAddr
	newCfg.AdminAddr = oldCfg.AdminAddr
	newCfg.Strategy = oldCfg.Strategy
	newCfg.HashKey = oldCfg.HashKey
	newCfg.MaxClients = oldCfg.MaxClients
	newCfg.OTel = oldCfg.OTel

	if len(newCfg.Upstreams) == 0 {
		return fmt.Errorf("config contains no upstreams; reload rejected to preserve current pool")
	}

	newUps, err := newCfg.BuildUpstreams()
	if err != nil {
		return fmt.Errorf("build upstreams: %w", err)
	}

	newGateNeeded := oldCfg.Backpressure != newCfg.Backpressure ||
		oldCfg.AdmissionWaitTimeout != newCfg.AdmissionWaitTimeout

	var newGate admission.Gate
	if newGateNeeded {
		active := lb.tracker.Count()
		if active != 0 {
			lb.log.Warn("reload_field_not_reloadable_while_sessions_active",
				"fields", "backpressure,admission_wait_timeout",
				"active_sessions", active,
			)
			newCfg.Backpressure = oldCfg.Backpressure
			newCfg.AdmissionWaitTimeout = oldCfg.AdmissionWaitTimeout
		} else {
			newGate = admission.New(newCfg, lb.log, lb.tracker)
		}
	}

	newDialer, err := transport.NewFromConfig(newCfg)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}

	// Validate all artifacts before committing. Config and dialer are only
	// written after setUpstreams succeeds, preventing partial-update states.
	if err := lb.setUpstreams(newUps); err != nil {
		return fmt.Errorf("set upstreams: %w", err)
	}
	lb.log.Info("reload_upstreams_applied", "count", len(newUps))

	lb.cfgMu.Lock()
	lb.cfg = newCfg
	lb.dialer = newDialer
	lb.cfgMu.Unlock()

	if newGate != nil {
		lb.setGate(newGate)
		lb.metrics.BackpressureInfo.Reset()
		lb.metrics.BackpressureInfo.WithLabelValues(string(newCfg.Backpressure)).Set(1)
		lb.log.Info("reload_backpressure_applied",
			"strategy", string(newCfg.Backpressure),
			"wait_timeout", newCfg.AdmissionWaitTimeout.String(),
		)
	}

	lb.log.Info("reload_complete")
	return nil
}

// Reload re-reads the config file and applies safely hot-swappable fields.
// All application logic is delegated to reloadFromConfig.
func (lb *LoadBalancer) Reload() error {
	if lb.configPath == "" {
		return fmt.Errorf("reload: no config path was supplied at startup")
	}

	newCfg, err := config.LoadFile(lb.configPath)
	if err != nil {
		return fmt.Errorf("reload: %w", err)
	}

	return lb.reloadFromConfig(newCfg)
}

func warnImmutable(log *slog.Logger, name, old, new string) {
	if old != new {
		log.Warn("reload_field_not_reloadable",
			"field", name,
			"old", old,
			"new", new,
		)
	}
}

func warnImmutableInt(log *slog.Logger, name string, old, new int) {
	if old != new {
		log.Warn("reload_field_not_reloadable",
			"field", name,
			"old", old,
			"new", new,
		)
	}
}
