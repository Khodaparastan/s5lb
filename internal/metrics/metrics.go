// Package metrics wires up the Prometheus collectors for socks5lb.
package metrics

import (
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the full collector set exposed on the admin port.
type Metrics struct {
	BuildInfo        *prometheus.GaugeVec
	StrategyInfo     *prometheus.GaugeVec
	BackpressureInfo *prometheus.GaugeVec

	AcceptedTotal     prometheus.Counter
	RejectedTotal     *prometheus.CounterVec
	BackpressureEvict prometheus.Counter

	ActiveSessions  prometheus.Gauge
	AdmissionInFly  prometheus.Gauge
	QueueDepth      prometheus.Gauge
	QueueWaitSec    prometheus.Histogram
	SessionDuration prometheus.Histogram
	SessionBytes    *prometheus.CounterVec

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
}

// New builds and registers every collector on the supplied registry.
func New(reg prometheus.Registerer, version, commit, buildDate string) *Metrics {
	latencyBuckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	durBuckets := []float64{.1, 1, 10, 30, 60, 300, 900, 1800, 3600}

	m := &Metrics{
		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_build_info",
			Help: "Build info; always 1.",
		}, []string{"version", "commit", "goversion", "build_date"}),

		StrategyInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_strategy_info",
			Help: "Active load-balancing strategy (always 1).",
		}, []string{"strategy"}),

		BackpressureInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_backpressure_info",
			Help: "Active backpressure strategy (always 1).",
		}, []string{"strategy"}),

		AcceptedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "socks5lb_connections_accepted_total",
			Help: "TCP connections accepted on the frontend listener.",
		}),
		RejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_connections_rejected_total",
			Help: "Connections rejected/terminated, partitioned by reason.",
		}, []string{"reason"}),
		BackpressureEvict: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "socks5lb_backpressure_evictions_total",
			Help: "Sessions evicted to admit a new client.",
		}),

		ActiveSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "socks5lb_active_sessions",
			Help: "TCP tunneling sessions currently active.",
		}),
		AdmissionInFly: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "socks5lb_admission_inflight",
			Help: "Clients currently holding an admission slot.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "socks5lb_queue_depth",
			Help: "Clients waiting for an upstream slot.",
		}),
		QueueWaitSec: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "socks5lb_queue_wait_seconds",
			Help:    "Time spent queued waiting for an upstream slot.",
			Buckets: latencyBuckets,
		}),
		SessionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "socks5lb_session_duration_seconds",
			Help:    "End-to-end tunneled session duration.",
			Buckets: durBuckets,
		}),
		SessionBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_session_bytes_total",
			Help: "Bytes copied during TCP tunneling, by direction.",
		}, []string{"direction"}),

		SocksRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_socks_request_total",
			Help: "SOCKS5 requests by address type and outcome.",
		}, []string{"atyp", "result"}),
		SocksReply: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_socks_reply_total",
			Help: "SOCKS5 replies sent to clients, by code.",
		}, []string{"code"}),

		UpSelected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_upstream_selected_total",
			Help: "Sessions selecting each upstream.",
		}, []string{"upstream"}),
		UpActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_upstream_active",
			Help: "Active sessions per upstream.",
		}, []string{"upstream"}),
		UpHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_upstream_healthy",
			Help: "1 if upstream is healthy, 0 otherwise.",
		}, []string{"upstream"}),
		UpSessions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_upstream_sessions_total",
			Help: "Total sessions dispatched to each upstream.",
		}, []string{"upstream"}),
		UpFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_upstream_failures_total",
			Help: "Upstream failures by stage.",
		}, []string{"upstream", "stage"}),
		UpDialSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "socks5lb_upstream_dial_seconds",
			Help:    "Upstream TCP dial latency.",
			Buckets: latencyBuckets,
		}, []string{"upstream"}),
		UpHandshake: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "socks5lb_upstream_handshake_seconds",
			Help:    "Upstream SOCKS5 handshake latency.",
			Buckets: latencyBuckets,
		}, []string{"upstream"}),
		UpProbeSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "socks5lb_upstream_probe_seconds",
			Help:    "Active health probe latency.",
			Buckets: latencyBuckets,
		}, []string{"upstream", "result"}),
		UpLatencyEWMA: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "socks5lb_upstream_ewma_latency_seconds",
			Help: "EWMA of (dial+handshake) latency per upstream, seconds.",
		}, []string{"upstream"}),

		UDPAssocActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "socks5lb_udp_associations_active",
			Help: "Active UDP_ASSOCIATE sessions.",
		}),
		UDPSessionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "socks5lb_udp_session_duration_seconds",
			Help:    "UDP_ASSOCIATE session duration.",
			Buckets: durBuckets,
		}),
		UDPPackets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_udp_packets_total",
			Help: "UDP datagrams relayed, by direction.",
		}, []string{"direction"}),
		UDPBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_udp_bytes_total",
			Help: "UDP payload bytes relayed, by direction.",
		}, []string{"direction"}),
		UDPDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "socks5lb_udp_dropped_total",
			Help: "UDP datagrams dropped, by reason.",
		}, []string{"reason"}),
	}

	reg.MustRegister(
		m.BuildInfo, m.StrategyInfo, m.BackpressureInfo,
		m.AcceptedTotal, m.RejectedTotal, m.BackpressureEvict,
		m.ActiveSessions, m.AdmissionInFly,
		m.QueueDepth, m.QueueWaitSec,
		m.SessionDuration, m.SessionBytes,
		m.SocksRequest, m.SocksReply,
		m.UpSelected, m.UpActive, m.UpHealthy,
		m.UpSessions, m.UpFailures,
		m.UpDialSec, m.UpHandshake, m.UpProbeSec,
		m.UpLatencyEWMA,
		m.UDPAssocActive, m.UDPSessionDuration,
		m.UDPPackets, m.UDPBytes, m.UDPDropped,
	)

	m.BuildInfo.WithLabelValues(version, commit, runtime.Version(), buildDate).Set(1)
	return m
}

// RegisterRuntime registers the standard Go + process collectors.
func RegisterRuntime(reg prometheus.Registerer) {
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}
