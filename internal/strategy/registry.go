package strategy

import (
	"fmt"
	"strings"
)

// New builds the selector identified by `name`.  `poolSize` is required by
// strategies that pre-allocate per-upstream state (e.g. WRR).
func New(name string, poolSize int) (Selector, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "least-active", "leastconn":
		return NewLeastActive(), nil
	case "round-robin", "rr":
		return NewRoundRobin(), nil
	case "weighted-round-robin", "wrr":
		return NewWRR(poolSize), nil
	case "random":
		return NewRandom(), nil
	case "weighted-random", "wrandom":
		return NewWeightedRandom(), nil
	case "p2c", "power-of-two":
		return NewP2C(), nil
	case "least-latency", "ll":
		return NewLeastLatency(), nil
	case "consistent-hash", "hash":
		return NewConsistentHash(), nil
	case "priority-failover", "priority", "failover":
		return NewPriorityFailover(), nil
	default:
		return nil, fmt.Errorf("unknown strategy %q", name)
	}
}

// Names lists all registered strategy names.
func Names() []string {
	return []string{
		"least-active", "round-robin", "weighted-round-robin",
		"random", "weighted-random", "p2c", "least-latency",
		"consistent-hash", "priority-failover",
	}
}
