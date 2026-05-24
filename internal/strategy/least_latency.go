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
	bestID := ""
	bestScore := math.MaxFloat64
	tieCount := 0

	for _, u := range pool {
		if !u.Healthy || u.Active >= maxPer {
			continue
		}
		lat := u.EWMALatency
		if lat <= 0 {
			lat = 1e-9
		}
		score := lat * float64(1+u.Active)

		if bestID == "" || score < bestScore {
			bestID = u.ID
			bestScore = score
			tieCount = 1
		} else if score == bestScore {
			// Reservoir sampling: replace with probability 1/(tieCount+1).
			tieCount++
			if rand.IntN(tieCount) == 0 {
				bestID = u.ID
			}
		}
	}
	return bestID
}
