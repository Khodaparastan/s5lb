package strategy

import "github.com/khodaparastan/socks5lb/internal/upstream"

// PriorityFailoverSelector assumes `pool` is pre-sorted by Priority ascending.
// Returns the first eligible upstream — higher-priority peers are always
// preferred until they lose capacity or health.
type PriorityFailoverSelector struct{}

func NewPriorityFailover() *PriorityFailoverSelector { return &PriorityFailoverSelector{} }
func (s *PriorityFailoverSelector) Name() string     { return "priority-failover" }

func (s *PriorityFailoverSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	for _, u := range pool {
		if u.Healthy && u.Active < max {
			return u
		}
	}
	return nil
}
