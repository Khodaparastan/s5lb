package upstream

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// Upstream represents a backend SOCKS5 proxy with state and counters.
//
// Mutable fields under the balancer's lock:
//   - Active, Healthy, LastFailureTS, ConsecutiveFailures, FirstFailureTS
//
// Lock-free (atomic):
//   - TotalSessions, TotalFailures, ewmaBits
type Upstream struct {
	Host     string
	Port     int
	Username string
	Password string

	Weight   int // default 1; used by weighted strategies
	Priority int // default 0; lower value = higher priority

	// Guarded by balancer mutex:
	Active              int
	Healthy             bool
	LastFailureTS       time.Time
	ConsecutiveFailures int
	FirstFailureTS      time.Time

	// Lock-free counters:
	TotalSessions atomic.Uint64
	TotalFailures atomic.Uint64

	// EWMA of (dial + handshake) latency in seconds.
	ewmaBits atomic.Uint64
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

const ewmaAlpha = 0.2

// EWMALatency returns the exponentially-weighted moving average latency.
// Returns 0 if no samples have been observed yet.
func (u *Upstream) EWMALatency() float64 {
	return math.Float64frombits(u.ewmaBits.Load())
}

// ObserveLatency updates the EWMA with a new latency sample.
func (u *Upstream) ObserveLatency(d time.Duration) {
	sample := d.Seconds()
	for {
		prev := u.ewmaBits.Load()
		cur := math.Float64frombits(prev)
		var next float64
		if cur == 0 {
			next = sample
		} else {
			next = ewmaAlpha*sample + (1-ewmaAlpha)*cur
		}
		if u.ewmaBits.CompareAndSwap(prev, math.Float64bits(next)) {
			return
		}
	}
}
