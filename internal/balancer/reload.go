package balancer

import (
	"fmt"
	"log/slog"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/config"
)

// Reload re-reads the config file and applies the subset of settings that
// are safely hot-swappable:
//
//   - upstream pool (added/removed/modified)
//   - backpressure strategy + admission wait timeout
//   - health/retry/connect/handshake/queue/failure/idle timeouts
//   - drain timeouts
//   - log level
//
// NOT reloadable without restart: ListenAddr, AdminAddr, Strategy,
// HashKey, MaxClients, OTel config. Changes to these are logged and ignored.
func (lb *LoadBalancer) Reload() error {
	if lb.configPath == "" {
		return fmt.Errorf("reload: no config path was supplied at startup")
	}
	newCfg, err := config.LoadFile(lb.configPath)
	if err != nil {
		return fmt.Errorf("reload: %w", err)
	}
	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("reload: validate: %w", err)
	}

	oldCfg := lb.Config()

	// Warn on non-reloadable changes.
	warnImmutable(lb.log, "listen", oldCfg.ListenAddr, newCfg.ListenAddr)
	warnImmutable(lb.log, "admin", oldCfg.AdminAddr, newCfg.AdminAddr)
	warnImmutable(lb.log, "strategy", oldCfg.Strategy, newCfg.Strategy)
	warnImmutable(lb.log, "hash_key", string(oldCfg.HashKey), string(newCfg.HashKey))
	warnImmutableInt(lb.log, "max_clients", oldCfg.MaxClients, newCfg.MaxClients)

	// Preserve non-reloadable fields to keep runtime state consistent.
	newCfg.ListenAddr = oldCfg.ListenAddr
	newCfg.AdminAddr = oldCfg.AdminAddr
	newCfg.Strategy = oldCfg.Strategy
	newCfg.HashKey = oldCfg.HashKey
	newCfg.MaxClients = oldCfg.MaxClients
	newCfg.OTel = oldCfg.OTel

	// Swap config.
	lb.cfgMu.Lock()
	lb.cfg = newCfg
	lb.cfgMu.Unlock()

	// Rebuild upstream pool.
	if len(newCfg.Upstreams) > 0 {
		ups, err := newCfg.BuildUpstreams()
		if err != nil {
			return fmt.Errorf("reload: build upstreams: %w", err)
		}
		lb.setUpstreams(ups)
		lb.log.Info("reload_upstreams_applied", "count", len(ups))
	}

	// Rebuild gate if backpressure settings changed.
	if oldCfg.Backpressure != newCfg.Backpressure ||
		oldCfg.AdmissionWaitTimeout != newCfg.AdmissionWaitTimeout {
		lb.gate = admission.New(newCfg, lb.log, lb.tracker)
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

func warnImmutable(log *slog.Logger, name, old, new string) {
	if old != new {
		log.Warn("reload_field_not_reloadable",
			"field", name, "old", old, "new", new)
	}
}
func warnImmutableInt(log *slog.Logger, name string, old, new int) {
	if old != new {
		log.Warn("reload_field_not_reloadable",
			"field", name, "old", old, "new", new)
	}
}
