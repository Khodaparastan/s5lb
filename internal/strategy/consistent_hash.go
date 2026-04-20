package strategy

import (
	"hash/fnv"
	"sort"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// ConsistentHashSelector uses Rendezvous (HRW) hashing for session affinity.
// On failure of the top-ranked upstream, falls through to the next highest.
type ConsistentHashSelector struct{}

func NewConsistentHash() *ConsistentHashSelector { return &ConsistentHashSelector{} }
func (s *ConsistentHashSelector) Name() string   { return "consistent-hash" }

func (s *ConsistentHashSelector) Pick(sc SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	if len(pool) == 0 {
		return ""
	}
	key := sc.HashInput()
	type rank struct {
		u upstream.Snapshot
		h uint64
	}
	ranks := make([]rank, 0, len(pool))
	for _, u := range pool {
		ranks = append(ranks, rank{u, hrw(key, u.ID)})
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].h > ranks[j].h })
	for _, r := range ranks {
		if r.u.Healthy && r.u.Active < maxPer {
			return r.u.ID
		}
	}
	return ""
}

func hrw(key, target string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(target))
	return h.Sum64()
}
