package strategy

import (
	"github.com/khodaparastan/s5lb/internal/upstream"
)

// PriorityFailoverSelector prefers lower-Priority upstreams. Within the same
// priority class, ties fall through in pool order.
// The pool is already sorted by priority ascending by setUpstreams, so this
// is an allocation-free linear scan.
type PriorityFailoverSelector struct{}

func NewPriorityFailover() *PriorityFailoverSelector { return &PriorityFailoverSelector{} }
func (s *PriorityFailoverSelector) Name() string     { return "priority-failover" }

func (s *PriorityFailoverSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	for _, u := range pool {
		if u.Healthy && u.Active < maxPer {
			return u.ID
		}
	}
	return ""
}
