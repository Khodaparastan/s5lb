package strategy

import (
	"math/rand/v2"

	"github.com/khodaparastan/s5lb/internal/upstream"
)

// RandomSelector picks a uniformly-random eligible upstream.
type RandomSelector struct{}

func NewRandom() *RandomSelector       { return &RandomSelector{} }
func (s *RandomSelector) Name() string { return "random" }

func (s *RandomSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	cands := eligible(pool, maxPer)
	if len(cands) == 0 {
		return ""
	}
	return cands[rand.IntN(len(cands))].ID
}
