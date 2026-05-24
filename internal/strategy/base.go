// Package strategy provides pluggable upstream-selection algorithms.
//
// Strategies are pure functions of a []upstream.Snapshot plus a SelectCtx.
// They return the chosen upstream's ID (empty string == no eligible upstream).
// The balancer resolves ID -> *Upstream when assigning a slot.
package strategy

import (
	"fmt"
	"net"

	"github.com/khodaparastan/s5lb/internal/config"
	"github.com/khodaparastan/s5lb/internal/upstream"
)

// Selector is the interface every load-balancing strategy implements.
type Selector interface {
	Name() string
	// Pick returns the ID of a chosen upstream, or "" if no upstream in
	// `pool` is eligible.
	Pick(sc SelectCtx, pool []upstream.Snapshot, maxPer int) string
}

// SelectCtx carries per-request metadata available to strategies.
type SelectCtx struct {
	ClientAddr net.Addr
	DstHost    string
	DstPort    uint16
	HashKey    config.HashKey
}

// HashInput returns the string used as the hash key for affinity strategies.
func (sc SelectCtx) HashInput() string {
	switch sc.HashKey {
	case config.HashDestination:
		return fmt.Sprintf("%s:%d", sc.DstHost, sc.DstPort)
	case config.HashDestHost:
		return sc.DstHost
	default: // HashClientIP
		if sc.ClientAddr == nil {
			return ""
		}
		host, _, err := net.SplitHostPort(sc.ClientAddr.String())
		if err != nil {
			return sc.ClientAddr.String()
		}
		return host
	}
}

// eligible returns the subset of the pool that is healthy and below the
// per-upstream max active cap.
func eligible(pool []upstream.Snapshot, maxPer int) []upstream.Snapshot {
	out := make([]upstream.Snapshot, 0, len(pool))
	for _, s := range pool {
		if s.Healthy && s.Active < maxPer {
			out = append(out, s)
		}
	}
	return out
}
