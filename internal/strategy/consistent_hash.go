package strategy

import (
	"hash/fnv"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// ConsistentHashSelector uses Rendezvous (HRW) hashing for session affinity.
// On failure of the top-ranked upstream, falls through to the next highest.
// Uses an allocation-free single-pass scan instead of sorting per pick.
type ConsistentHashSelector struct{}

func NewConsistentHash() *ConsistentHashSelector { return &ConsistentHashSelector{} }
func (s *ConsistentHashSelector) Name() string   { return "consistent-hash" }

func (s *ConsistentHashSelector) Pick(sc SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	if len(pool) == 0 {
		return ""
	}
	key := sc.HashInput()

	bestID := ""
	var bestH uint64

	for _, u := range pool {
		if !u.Healthy || u.Active >= maxPer {
			continue
		}
		h := hrw(key, u.ID)
		if bestID == "" || h > bestH {
			bestID = u.ID
			bestH = h
		}
	}
	return bestID
}

func hrw(key, target string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(target))
	return h.Sum64()
}
