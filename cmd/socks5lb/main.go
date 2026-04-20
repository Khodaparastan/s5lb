// Command socks5lb is a SOCKS5 load-balancing proxy with pluggable strategies,
// configurable backpressure, UDP ASSOCIATE, OpenTelemetry tracing, Prometheus
// metrics, two-phase graceful drain, and SIGHUP hot reload.
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
	"github.com/khodaparastan/socks5lb/internal/telemetry"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// Build stamps (ldflags).
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

type upstreamList []string

func (l *upstreamList) String() string     { return strings.Join(*l, ",") }
func (l *upstreamList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	// --- Flags ---
	configPath := flag.String("config", "", "path to YAML config file (flags override)")
	listen := flag.String("listen", "", "frontend listen addr (overrides config)")
	admAddr := flag.String("admin-addr", "", "admin listen addr (overrides config)")

	var upstreamsFlag upstreamList
	flag.Var(&upstreamsFlag, "upstream",
		"upstream (repeatable); forms: host:port | user:pass@host:port | "+
			"socks5://user:pass@host:port?weight=N&priority=N | host:port#w=N,p=N,id=foo")

	strategyFlag := flag.String("strategy", "", "balancing strategy: "+strings.Join(strategy.Names(), " | "))
	hashKeyFlag := flag.String("hash-key", "", "client-ip | destination | destination-host")
	backpressureFlag := flag.String("backpressure", "",
		"reject | wait | drop-oldest | drop-lowest-priority")

	maxPerProxy := flag.Int("max-per-proxy", 0, "override max_per_proxy")
	maxClients := flag.Int("max-clients", 0, "override max_clients")
	logLevel := flag.String("log-level", "", "debug|info|warn|error")
	logFormat := flag.String("log-format", "", "json|text")

	udpEnabled := flag.Bool("udp", true, "enable UDP_ASSOCIATE")
	otelEnabled := flag.Bool("otel", false, "enable OpenTelemetry tracing")
	otelEndpoint := flag.String("otel-endpoint", "", "OTLP gRPC endpoint (e.g., otel-collector:4317)")
	otelInsecure := flag.Bool("otel-insecure", true, "disable TLS to OTLP endpoint")

	showVersion := flag.Bool("version", false, "print version and exit")
	listStrategies := flag.Bool("list-strategies", false, "list strategies and exit")

	flag.Parse()

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

	// --- Load config ---
	var cfg config.Config
	if *configPath != "" {
		loaded, err := config.LoadFile(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading config: %v\n", err)
			os.Exit(2)
		}
		cfg = loaded
	} else {
		cfg = config.Defaults()
	}

	// Flag overrides.
	applyFlagOverrides(&cfg,
		*listen, *admAddr, *strategyFlag, *hashKeyFlag, *backpressureFlag,
		*maxPerProxy, *maxClients, *logLevel, *logFormat,
		*udpEnabled, *otelEnabled, *otelEndpoint, *otelInsecure,
	)

	// Merge flag upstreams into config upstreams.
	upstreams, err := cfg.BuildUpstreams()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error building upstreams from config: %v\n", err)
		os.Exit(2)
	}
	for _, spec := range upstreamsFlag {
		u, err := config.ParseUpstream(spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid upstream %q: %v\n", spec, err)
			os.Exit(2)
		}
		upstreams = append(upstreams, u)
	}
	if len(upstreams) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one upstream is required (config or -upstream)")
		flag.Usage()
		os.Exit(2)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(2)
	}

	// --- Logger ---
	log := logging.New(cfg.LogLevel, cfg.LogFormat, "socks5lb", version)
	log.Info("starting",
		"version", version, "commit", commit, "build_date", buildDate,
		"strategy", cfg.Strategy, "backpressure", string(cfg.Backpressure),
		"udp_enabled", cfg.UDPEnabled, "otel_enabled", cfg.OTel.Enabled,
	)

	// --- Strategy ---
	sel, err := strategy.New(cfg.Strategy)
	if err != nil {
		log.Error("invalid_strategy", "err", err.Error())
		os.Exit(2)
	}

	// --- Metrics ---
	reg := prometheus.NewRegistry()
	metrics.RegisterRuntime(reg)
	m := metrics.New(reg, version, commit, buildDate)

	// --- OTel ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prov, err := telemetry.Init(ctx, cfg.OTel, version, commit)
	if err != nil {
		log.Error("otel_init_failed", "err", err.Error())
		os.Exit(2)
	}
	defer func() {
		sctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
		_ = prov.Shutdown(sctx)
		cc()
	}()

	// --- Balancer ---
	lb := balancer.New(cfg, *configPath, log, m, prov.Tracer, upstreams, sel)

	// --- Admin ---
	var adm *admin.Server
	if cfg.AdminAddr != "" {
		adm = admin.New(cfg.AdminAddr, lb, lb, reg, log, version, commit, buildDate)
		adm.Start()
	}

	// --- Signals / lifecycle ---
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	serveErr := make(chan error, 1)
	go func() { serveErr <- lb.Serve() }()

	for {
		select {
		case sig := <-sigCh:
			switch sig {
			case syscall.SIGHUP:
				log.Info("signal_received_reload")
				if err := lb.Reload(); err != nil {
					log.Error("reload_failed", "err", err.Error())
				}
				continue
			default:
				log.Info("signal_received", "signal", sig.String())
				lb.Shutdown()
				if adm != nil {
					sctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
					adm.Stop(sctx)
					cc()
				}
				if err := <-serveErr; err != nil {
					log.Error("server_error", "err", err.Error())
					os.Exit(1)
				}
				return
			}
		case err := <-serveErr:
			if err != nil {
				log.Error("server_error", "err", err.Error())
				os.Exit(1)
			}
			return
		}
	}

	// unreachable
	_ = upstream.Upstream{}
}

// applyFlagOverrides merges non-zero flag values over cfg.
func applyFlagOverrides(
	cfg *config.Config,
	listen, admAddr, strat, hashKey, backpressure string,
	maxPer, maxCli int,
	logLevel, logFormat string,
	udpEnabled bool,
	otelEnabled bool, otelEndpoint string, otelInsecure bool,
) {
	if listen != "" {
		cfg.ListenAddr = listen
	}
	if admAddr != "" {
		cfg.AdminAddr = admAddr
	}
	if strat != "" {
		cfg.Strategy = strat
	}
	if hashKey != "" {
		cfg.HashKey = config.HashKey(hashKey)
	}
	if backpressure != "" {
		cfg.Backpressure = config.BackpressureStrategy(backpressure)
	}
	if maxPer > 0 {
		cfg.MaxPerProxy = maxPer
	}
	if maxCli > 0 {
		cfg.MaxClients = maxCli
	}
	if logLevel != "" {
		cfg.LogLevel = logging.ParseLevel(logLevel)
	}
	if logFormat != "" {
		cfg.LogFormat = logFormat
	}
	cfg.UDPEnabled = udpEnabled
	if otelEnabled {
		cfg.OTel.Enabled = true
	}
	if otelEndpoint != "" {
		cfg.OTel.Endpoint = otelEndpoint
		cfg.OTel.Enabled = true
	}
	if !otelInsecure {
		cfg.OTel.Insecure = false
	} else {
		cfg.OTel.Insecure = true
	}
}
