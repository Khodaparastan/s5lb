package strategy

import "github.com/khodaparastan/socks5lb/internal/upstream"

// P2CSelector — "power of two choices": pick two random eligible upstreams
// and send to whichever has fewer active connections.  Near-optimal load
// balancing without the O(n) scan of least-active.
type P2CSelector struct{ rng *lockedRand }

func NewP2C() *P2CSelector          { return &P2CSelector{rng: newLockedRand()} }
func (s *P2CSelector) Name() string { return "p2c" }

func (s *P2CSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	cands := eligible(pool, max)
	switch len(cands) {
	case 0:
		return nil
	case 1:
		return cands[0]
	}
	i := s.rng.Intn(len(cands))
	j := s.rng.Intn(len(cands) - 1)
	if j >= i {
		j++
	}
	a, b := cands[i], cands[j]
	if a.Active <= b.Active {
		return a
	}
	return b
}
