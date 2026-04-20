package strategy

import (
	"hash/fnv"
	"sort"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// ConsistentHashSelector uses Rendezvous (HRW) hashing.  For a stable key,
// upstream order is deterministic; on failure the next-highest ranked
// eligible upstream is selected.  Provides session affinity without a ring.
type ConsistentHashSelector struct{}

func NewConsistentHash() *ConsistentHashSelector { return &ConsistentHashSelector{} }
func (s *ConsistentHashSelector) Name() string   { return "consistent-hash" }

func (s *ConsistentHashSelector) Pick(sc SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	if len(pool) == 0 {
		return nil
	}
	key := sc.HashInput()
	type rank struct {
		u *upstream.Upstream
		h uint64
	}
	ranks := make([]rank, 0, len(pool))
	for _, u := range pool {
		ranks = append(ranks, rank{u, hrw(key, u.Addr())})
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].h > ranks[j].h })
	for _, r := range ranks {
		if r.u.Healthy && r.u.Active < max {
			return r.u
		}
	}
	return nil
}

func hrw(key, target string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(target))
	return h.Sum64()
}
