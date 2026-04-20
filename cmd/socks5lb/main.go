// socks5lb — a SOCKS5 load-balancing proxy with active health checks,
// per-upstream concurrency caps, FIFO waiter queue, circuit breaker,
// graceful drain, structured logging, Prometheus metrics, and pluggable
// balancing strategies.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/khodaparastan/socks5lb/internal/admin"
	"github.com/khodaparastan/socks5lb/internal/balancer"
	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/logging"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// Build stamp — wired via -ldflags at build time.
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

// upstreamList implements flag.Value for repeated -upstream flags.
type upstreamList []string

func (l *upstreamList) String() string     { return strings.Join(*l, ",") }
func (l *upstreamList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	cfg := config.Defaults()

	// --- Flags -------------------------------------------------------------
	var upstreamsFlag upstreamList
	flag.Var(&upstreamsFlag, "upstream",
		"upstream; repeatable. Forms:\n"+
			"  host:port\n"+
			"  user:pass@host:port\n"+
			"  socks5://user:pass@host:port?weight=N&priority=N\n"+
			"  host:port#w=N,p=N")

	listen := flag.String("listen", cfg.ListenAddr, "data-path listen address")
	admAddr := flag.String("admin-addr", cfg.AdminAddr,
		"admin listen (metrics/health/pprof); empty to disable")

	maxPerProxy := flag.Int("max-per-proxy", cfg.MaxPerProxy,
		"max active connections per upstream")
	maxClients := flag.Int("max-clients", cfg.MaxClients,
		"global cap on in-flight clients")

	healthInterval := flag.Duration("health-interval", cfg.HealthInterval,
		"upstream health-check interval")
	retryBackoff := flag.Duration("retry-backoff", cfg.RetryBackoff,
		"backoff before re-probing an unhealthy upstream")
	connectTimeout := flag.Duration("connect-timeout", cfg.ConnectTimeout,
		"upstream TCP dial timeout")
	handshakeTimeout := flag.Duration("handshake-timeout", cfg.HandshakeTimeout,
		"SOCKS5 handshake timeout")
	queueWait := flag.Duration("queue-wait-timeout", cfg.QueueWaitTimeout,
		"max time a client waits for an upstream slot")
	failThreshold := flag.Int("failure-threshold", cfg.FailureThreshold,
		"consecutive failures before circuit opens")
	failWindow := flag.Duration("failure-window", cfg.FailureWindow,
		"sliding window for consecutive-failure counter")
	idleTimeout := flag.Duration("idle-timeout", cfg.IdleTimeout,
		"per-direction idle timeout during tunneling (0 disables; enabling disables splice)")
	drainTimeout := flag.Duration("drain-timeout", cfg.DrainTimeout,
		"shutdown drain timeout")
	keepAlive := flag.Bool("tcp-keepalive", cfg.TCPKeepAlive,
		"enable TCP keepalive on tunneled sockets")

	strategyFlag := flag.String("strategy", cfg.Strategy,
		"balancing strategy: "+strings.Join(strategy.Names(), " | "))
	hashKeyFlag := flag.String("hash-key", "client-ip",
		"for consistent-hash: client-ip | dst | dst-host")

	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	logFormat := flag.String("log-format", cfg.LogFormat, "json | text")

	showVersion := flag.Bool("version", false, "print version and exit")
	listStrategies := flag.Bool("list-strategies", false, "print strategies and exit")

	flag.Parse()

	// --- Early exits -------------------------------------------------------
	if *showVersion {
		fmt.Printf("socks5lb %s (%s) %s [%s]\n",
			version, commit, runtime.Version(), buildDate)
		return
	}
	if *listStrategies {
		for _, n := range strategy.Names() {
			fmt.Println(n)
		}
		return
	}
	if len(upstreamsFlag) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one --upstream is required")
		flag.Usage()
		os.Exit(2)
	}

	// --- Populate config ---------------------------------------------------
	hk, ok := strategy.ParseHashKey(*hashKeyFlag)
	if !ok {
		fmt.Fprintf(os.Stderr, "error: invalid --hash-key %q\n", *hashKeyFlag)
		os.Exit(2)
	}

	cfg.ListenAddr = *listen
	cfg.AdminAddr = *admAddr
	cfg.MaxPerProxy = *maxPerProxy
	cfg.MaxClients = *maxClients
	cfg.HealthInterval = *healthInterval
	cfg.RetryBackoff = *retryBackoff
	cfg.ConnectTimeout = *connectTimeout
	cfg.HandshakeTimeout = *handshakeTimeout
	cfg.QueueWaitTimeout = *queueWait
	cfg.FailureThreshold = *failThreshold
	cfg.FailureWindow = *failWindow
	cfg.IdleTimeout = *idleTimeout
	cfg.DrainTimeout = *drainTimeout
	cfg.TCPKeepAlive = *keepAlive
	cfg.Strategy = *strategyFlag
	cfg.HashKey = hk
	cfg.LogLevel = logging.ParseLevel(*logLevel)
	cfg.LogFormat = *logFormat

	// --- Logger ------------------------------------------------------------
	log := logging.New(cfg.LogLevel, cfg.LogFormat, "socks5lb", version)

	// --- Parse upstreams ---------------------------------------------------
	var upstreams []*upstream.Upstream
	for _, spec := range upstreamsFlag {
		u, err := config.ParseUpstream(spec)
		if err != nil {
			log.Error("invalid_upstream", "spec", spec, "err", err.Error())
			os.Exit(2)
		}
		upstreams = append(upstreams, u)
	}

	// --- Selector ----------------------------------------------------------
	sel, err := strategy.New(cfg.Strategy, len(upstreams))
	if err != nil {
		log.Error("invalid_strategy", "err", err.Error())
		os.Exit(2)
	}
	log.Info("strategy_selected",
		"strategy", sel.Name(),
		"hash_key", *hashKeyFlag)

	// --- Metrics -----------------------------------------------------------
	reg := prometheus.NewRegistry()
	metrics.RegisterRuntime(reg)
	m := metrics.New(reg, version, commit, buildDate)

	// --- Build balancer ----------------------------------------------------
	lb := balancer.New(cfg, log, m, upstreams, sel)

	// --- Admin server ------------------------------------------------------
	var adm *admin.Server
	if cfg.AdminAddr != "" {
		adm = admin.New(cfg.AdminAddr, lb, reg, log, version, commit, buildDate)
		adm.Start()
	}

	// --- Signals / lifecycle ----------------------------------------------
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	serveErr := make(chan error, 1)
	go func() { serveErr <- lb.Serve() }()

	select {
	case sig := <-sigCh:
		log.Info("signal_received", "signal", sig.String())
		lb.Shutdown()
		if adm != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			adm.Stop(ctx)
			cancel()
		}
		if err := <-serveErr; err != nil {
			log.Error("server_error", "err", err.Error())
			os.Exit(1)
		}
	case err := <-serveErr:
		if err != nil {
			log.Error("server_error", "err", err.Error())
			os.Exit(1)
		}
	}
}
