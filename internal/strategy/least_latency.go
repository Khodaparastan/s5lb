package strategy

import (
	"math"
	"math/rand/v2"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// LeastLatencySelector picks the upstream with lowest EWMA latency, lightly
// biased by active count. Unmeasured upstreams get a near-zero boost so they
// get sampled.
type LeastLatencySelector struct{}

func NewLeastLatency() *LeastLatencySelector { return &LeastLatencySelector{} }
func (s *LeastLatencySelector) Name() string { return "least-latency" }

func (s *LeastLatencySelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	cands := eligible(pool, maxPer)
	if len(cands) == 0 {
		return ""
	}
	var tied []upstream.Snapshot
	bestScore := math.MaxFloat64
	for _, u := range cands {
		lat := u.EWMALatency
		if lat <= 0 {
			lat = 1e-9
		}
		score := lat * float64(1+u.Active)
		switch {
		case score < bestScore:
			bestScore = score
			tied = tied[:0]
			tied = append(tied, u)
		case score == bestScore:
			tied = append(tied, u)
		}
	}
	if len(tied) == 1 {
		return tied[0].ID
	}
	return tied[rand.IntN(len(tied))].ID
}
