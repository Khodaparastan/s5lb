// Package upstream defines the upstream SOCKS5 proxy model.
//
// An Upstream is split into two parts:
//
//   - An immutable specification (Host, Port, credentials, Weight, Priority, ID).
//   - A mutable State (Active count, health, EWMA) protected by its own RWMutex.
//
// Selectors consume Snapshot value-copies of state, so strategy code never
// touches the mutex directly and cannot race.
package upstream

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
)

// Upstream is a backend SOCKS5 proxy.
type Upstream struct {
	// Immutable spec — set at construction, never mutated.
	ID       string // stable identifier (derived from addr+creds if empty)
	Host     string
	Port     int
	Username string
	Password string
	Weight   int // default 1
	Priority int // default 0; lower = higher priority

	// Mutable runtime state.
	State *State
}

// Addr returns the TCP address with correct bracketing for IPv6.
func (u *Upstream) Addr() string {
	if strings.Contains(u.Host, ":") && !strings.HasPrefix(u.Host, "[") {
		return fmt.Sprintf("[%s]:%d", u.Host, u.Port)
	}
	return fmt.Sprintf("%s:%d", u.Host, u.Port)
}

// Label is a display name for logs/metrics.
func (u *Upstream) Label() string { return u.Addr() }

// NormalizeID ensures the upstream has a stable ID. If unset, derives one
// from host:port (credentials intentionally excluded to keep IDs stable
// across password rotations).
func (u *Upstream) NormalizeID() {
	if u.ID != "" {
		return
	}
	h := sha1.Sum([]byte(u.Addr()))
	u.ID = hex.EncodeToString(h[:8])
}

// New creates an Upstream with a fresh State.
func New(host string, port int, user, pass string, weight, priority int) *Upstream {
	u := &Upstream{
		Host:     host,
		Port:     port,
		Username: user,
		Password: pass,
		Weight:   weight,
		Priority: priority,
		State:    NewState(),
	}
	u.NormalizeID()
	return u
}
