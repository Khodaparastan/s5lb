package strategy

import (
	"sort"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// PriorityFailoverSelector prefers lower-Priority upstreams. Within the same
// priority class, ties fall through in pool order.
type PriorityFailoverSelector struct{}

func NewPriorityFailover() *PriorityFailoverSelector { return &PriorityFailoverSelector{} }
func (s *PriorityFailoverSelector) Name() string     { return "priority-failover" }

func (s *PriorityFailoverSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	// Copy + sort by priority ascending; stable so same-priority order is
	// preserved as provided.
	sorted := make([]upstream.Snapshot, len(pool))
	copy(sorted, pool)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})
	for _, u := range sorted {
		if u.Healthy && u.Active < maxPer {
			return u.ID
		}
	}
	return ""
}
