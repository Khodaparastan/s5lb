package strategy

import (
	"math/rand/v2"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// P2CSelector — power-of-two choices. Samples two random eligible upstreams
// and picks the less-loaded. Near-optimal without the O(n) scan.
type P2CSelector struct{}

func NewP2C() *P2CSelector          { return &P2CSelector{} }
func (s *P2CSelector) Name() string { return "p2c" }

func (s *P2CSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	cands := eligible(pool, maxPer)
	switch len(cands) {
	case 0:
		return ""
	case 1:
		return cands[0].ID
	}
	i := rand.IntN(len(cands))
	j := rand.IntN(len(cands) - 1)
	if j >= i {
		j++
	}
	a, b := cands[i], cands[j]
	if a.Active <= b.Active {
		return a.ID
	}
	return b.ID
}
