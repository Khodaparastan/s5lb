package strategy

import (
	"fmt"
	"strings"
)

// New builds a Selector by name. Unknown names return a descriptive error.
func New(name string) (Selector, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "least-active", "leastconn":
		return NewLeastActive(), nil
	case "round-robin", "rr":
		return NewRoundRobin(), nil
	case "weighted-round-robin", "wrr":
		return NewWRR(), nil
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

// Names returns all registered strategy canonical names.
func Names() []string {
	return []string{
		"least-active",
		"round-robin",
		"weighted-round-robin",
		"random",
		"weighted-random",
		"p2c",
		"least-latency",
		"consistent-hash",
		"priority-failover",
	}
}
