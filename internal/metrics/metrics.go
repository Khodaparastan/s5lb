// Package metrics wires up the Prometheus collectors for socks5lb.
package metrics

import (
	"fmt"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the per-group collector set for a single upstream group.
type Metrics struct {
	StrategyInfo     *prometheus.GaugeVec
	BackpressureInfo *prometheus.GaugeVec

	AcceptedTotal     prometheus.Counter
	RejectedTotal     *prometheus.CounterVec
	BackpressureEvict prometheus.Counter

	ActiveSessions    prometheus.Gauge
	AdmissionInFlight prometheus.Gauge
	QueueDepth        prometheus.Gauge
	QueueWaitSec      prometheus.Histogram
	SessionDuration   prometheus.Histogram
	SessionBytes      *prometheus.CounterVec

	SocksRequest *prometheus.CounterVec
	SocksReply   *prometheus.CounterVec

	UpSelected    *prometheus.CounterVec
	UpActive      *prometheus.GaugeVec
	UpHealthy     *prometheus.GaugeVec
	UpSessions    *prometheus.CounterVec
	UpFailures    *prometheus.CounterVec
	UpDialSec     *prometheus.HistogramVec
	UpHandshake   *prometheus.HistogramVec
	UpProbeSec    *prometheus.HistogramVec
	UpLatencyEWMA *prometheus.GaugeVec

	// UDP_ASSOCIATE.
	UDPAssocActive     prometheus.Gauge
	UDPSessionDuration prometheus.Histogram
	UDPPackets         *prometheus.CounterVec
	UDPBytes           *prometheus.CounterVec
	UDPDropped         *prometheus.CounterVec

	// internal: used for clean unregistration on group removal.
	reg        prometheus.Registerer
	collectors []prometheus.Collector
}

// RegisterBuildInfo registers the global build-info gauge once on reg.
// Call this once at startup before calling New for any group.
func RegisterBuildInfo(reg prometheus.Registerer, version, commit, buildDate string) error {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "socks5lb_build_info",
		Help: "Build info; always 1.",
	}, []string{"version", "commit", "goversion", "build_date"})
	if err := reg.Register(g); err != nil {
		return fmt.Errorf("register build_info: %w", err)
	}
	g.WithLabelValues(version, commit, runtime.Version(), buildDate).Set(1)
	return nil
}

// New builds and registers every collector on the supplied registry.
// group is the logical group name this Metrics instance belongs to; it is
// embedded as a ConstLabel so callers never need to pass it on every observation.
// Use "default" for single-group deployments.
// Returns an error if any collector cannot be registered (e.g. duplicate group name).
func New(reg prometheus.Registerer, group, version, commit, buildDate string) (*Metrics, error) {
	latencyBuckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	durBuckets := []float64{.1, 1, 10, 30, 60, 300, 900, 1800, 3600}

	gl := prometheus.Labels{"group": group}

	m := &Metrics{
		StrategyInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "socks5lb_strategy_info",
			Help:        "Active load-balancing strategy (always 1).",
			ConstLabels: gl,
		}, []string{"strategy"}),

		BackpressureInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "socks5lb_backpressure_info",
			Help:        "Active backpressure strategy (always 1).",
			ConstLabels: gl,
		}, []string{"strategy"}),

		AcceptedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "socks5lb_connections_accepted_total",
			Help:        "TCP connections accepted on the frontend listener.",
			ConstLabels: gl,
		}),
		RejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_connections_rejected_total",
			Help:        "Connections rejected/terminated, partitioned by reason.",
			ConstLabels: gl,
		}, []string{"reason"}),
		BackpressureEvict: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "socks5lb_backpressure_evictions_total",
			Help:        "Sessions evicted to admit a new client.",
			ConstLabels: gl,
		}),

		ActiveSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "socks5lb_active_sessions",
			Help:        "TCP tunneling sessions currently active.",
			ConstLabels: gl,
		}),
		AdmissionInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "socks5lb_admission_inflight",
			Help:        "Clients currently holding an admission slot.",
			ConstLabels: gl,
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "socks5lb_queue_depth",
			Help:        "Clients waiting for an upstream slot.",
			ConstLabels: gl,
		}),
		QueueWaitSec: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "socks5lb_queue_wait_seconds",
			Help:        "Time spent queued waiting for an upstream slot.",
			Buckets:     latencyBuckets,
			ConstLabels: gl,
		}),
		SessionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "socks5lb_session_duration_seconds",
			Help:        "End-to-end tunneled session duration.",
			Buckets:     durBuckets,
			ConstLabels: gl,
		}),
		SessionBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_session_bytes_total",
			Help:        "Bytes copied during TCP tunneling, by direction.",
			ConstLabels: gl,
		}, []string{"direction"}),

		SocksRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_socks_request_total",
			Help:        "SOCKS5 requests by address type and outcome.",
			ConstLabels: gl,
		}, []string{"atyp", "result"}),
		SocksReply: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_socks_reply_total",
			Help:        "SOCKS5 replies sent to clients, by code.",
			ConstLabels: gl,
		}, []string{"code"}),

		UpSelected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_upstream_selected_total",
			Help:        "Sessions selecting each upstream.",
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "socks5lb_upstream_active",
			Help:        "Active sessions per upstream.",
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "socks5lb_upstream_healthy",
			Help:        "1 if upstream is healthy, 0 otherwise.",
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpSessions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_upstream_sessions_total",
			Help:        "Total sessions dispatched to each upstream.",
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_upstream_failures_total",
			Help:        "Upstream failures by stage.",
			ConstLabels: gl,
		}, []string{"upstream", "stage"}),
		UpDialSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "socks5lb_upstream_dial_seconds",
			Help:        "Upstream TCP dial latency.",
			Buckets:     latencyBuckets,
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpHandshake: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "socks5lb_upstream_handshake_seconds",
			Help:        "Upstream SOCKS5 handshake latency.",
			Buckets:     latencyBuckets,
			ConstLabels: gl,
		}, []string{"upstream"}),
		UpProbeSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "socks5lb_upstream_probe_seconds",
			Help:        "Active health probe latency.",
			Buckets:     latencyBuckets,
			ConstLabels: gl,
		}, []string{"upstream", "result"}),
		UpLatencyEWMA: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "socks5lb_upstream_ewma_latency_seconds",
			Help:        "EWMA of (dial+handshake) latency per upstream, seconds.",
			ConstLabels: gl,
		}, []string{"upstream"}),

		UDPAssocActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "socks5lb_udp_associations_active",
			Help:        "Active UDP_ASSOCIATE sessions.",
			ConstLabels: gl,
		}),
		UDPSessionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "socks5lb_udp_session_duration_seconds",
			Help:        "UDP_ASSOCIATE session duration.",
			Buckets:     durBuckets,
			ConstLabels: gl,
		}),
		UDPPackets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_udp_packets_total",
			Help:        "UDP datagrams relayed, by direction.",
			ConstLabels: gl,
		}, []string{"direction"}),
		UDPBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_udp_bytes_total",
			Help:        "UDP payload bytes relayed, by direction.",
			ConstLabels: gl,
		}, []string{"direction"}),
		UDPDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "socks5lb_udp_dropped_total",
			Help:        "UDP datagrams dropped, by reason.",
			ConstLabels: gl,
		}, []string{"reason"}),
	}

	m.reg = reg
	m.collectors = []prometheus.Collector{
		m.StrategyInfo, m.BackpressureInfo,
		m.AcceptedTotal, m.RejectedTotal, m.BackpressureEvict,
		m.ActiveSessions, m.AdmissionInFlight,
		m.QueueDepth, m.QueueWaitSec,
		m.SessionDuration, m.SessionBytes,
		m.SocksRequest, m.SocksReply,
		m.UpSelected, m.UpActive, m.UpHealthy,
		m.UpSessions, m.UpFailures,
		m.UpDialSec, m.UpHandshake, m.UpProbeSec,
		m.UpLatencyEWMA,
		m.UDPAssocActive, m.UDPSessionDuration,
		m.UDPPackets, m.UDPBytes, m.UDPDropped,
	}

	registered := make([]prometheus.Collector, 0, len(m.collectors))
	for _, c := range m.collectors {
		if err := reg.Register(c); err != nil {
			// Roll back any already-registered collectors.
			for _, rc := range registered {
				reg.Unregister(rc)
			}
			return nil, fmt.Errorf("register metrics for group %q: %w", group, err)
		}
		registered = append(registered, c)
	}

	return m, nil
}

// Unregister removes all collectors registered by this Metrics instance from
// the registry. Call this after draining a removed group so re-adding the
// same group name doesn't panic.
func (m *Metrics) Unregister() {
	if m == nil || m.reg == nil {
		return
	}
	for _, c := range m.collectors {
		m.reg.Unregister(c)
	}
}

// RegisterRuntime registers the standard Go + process collectors.
func RegisterRuntime(reg prometheus.Registerer) error {
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return fmt.Errorf("register go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return fmt.Errorf("register process collector: %w", err)
	}
	return nil
}
