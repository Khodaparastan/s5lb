package strategy

import (
	"math"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// LeastLatencySelector picks the upstream with the lowest EWMA of
// (dial + handshake) latency, lightly biased by current active count so
// ties route to the less-loaded peer.  Unmeasured upstreams are preferred
// once (so newcomers get a chance to gather samples).
type LeastLatencySelector struct{ rng *lockedRand }

func NewLeastLatency() *LeastLatencySelector {
	return &LeastLatencySelector{rng: newLockedRand()}
}
func (s *LeastLatencySelector) Name() string { return "least-latency" }

func (s *LeastLatencySelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	cands := eligible(pool, max)
	if len(cands) == 0 {
		return nil
	}
	var tied []*upstream.Upstream
	bestScore := math.MaxFloat64
	for _, u := range cands {
		lat := u.EWMALatency()
		if lat <= 0 {
			lat = 1e-9 // boost unmeasured so they get sampled
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
		return tied[0]
	}
	return tied[s.rng.Intn(len(tied))]
}
