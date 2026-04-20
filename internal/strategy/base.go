package strategy

import (
	"fmt"
	"math/rand"
	"net"
	"sync"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// Selector is the interface every load-balancing strategy must implement.
type Selector interface {
	Name() string
	Pick(sc SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream
}

// SelectCtx carries per-request metadata that strategies may consult.
type SelectCtx struct {
	ClientAddr net.Addr
	DstHost    string
	DstPort    uint16
	HashKey    HashKey
}

// HashInput returns a stable string key used by consistent-hash strategies.
func (sc SelectCtx) HashInput() string {
	switch sc.HashKey {
	case HashDestination:
		return fmt.Sprintf("%s:%d", sc.DstHost, sc.DstPort)
	default: // HashClientIP
		host, _, err := net.SplitHostPort(sc.ClientAddr.String())
		if err != nil {
			return sc.ClientAddr.String()
		}
		return host
	}
}

// HashKey selects which request attribute is used as the consistent-hash key.
type HashKey int

const (
	// HashClientIP hashes on the client's source IP address.
	HashClientIP HashKey = iota
	// HashDestination hashes on the destination address.
	HashDestination
)

// ParseHashKey converts a string flag value to a HashKey constant.
// Recognised values: "client-ip" (default), "destination".
func ParseHashKey(s string) (HashKey, bool) {
	switch s {
	case "client-ip", "":
		return HashClientIP, true
	case "destination":
		return HashDestination, true
	default:
		return HashClientIP, false
	}
}

// lockedRand is a goroutine-safe random number generator.
type lockedRand struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newLockedRand() *lockedRand {
	return &lockedRand{rng: rand.New(rand.NewSource(rand.Int63()))}
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rng.Intn(n)
	r.mu.Unlock()
	return v
}

func (r *lockedRand) Float64() float64 {
	r.mu.Lock()
	v := r.rng.Float64()
	r.mu.Unlock()
	return v
}

// eligible returns the subset of pool that is healthy and below the max
// active-connection limit.
func eligible(pool []*upstream.Upstream, max int) []*upstream.Upstream {
	out := make([]*upstream.Upstream, 0, len(pool))
	for _, u := range pool {
		if u.Healthy && u.Active < max {
			out = append(out, u)
		}
	}
	return out
}
