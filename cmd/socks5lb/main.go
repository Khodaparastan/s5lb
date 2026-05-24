// Command socks5lb is a SOCKS5 load-balancing proxy with pluggable strategies,
// configurable backpressure, UDP ASSOCIATE, OpenTelemetry tracing, Prometheus
// metrics, two-phase graceful drain, SIGHUP hot reload, and multi-group support.
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

	// --- Load multi-group config ---
	var mc config.MultiConfig
	if *configPath != "" {
		loaded, err := config.LoadMultiFile(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading config: %v\n", err)
			os.Exit(2)
		}
		mc = loaded
	} else {
		mc = config.MultiConfig{Config: config.Defaults()}
	}

	// Track which flags were explicitly set by the user.
	seenFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { seenFlags[f.Name] = true })

	// Apply flag overrides to the embedded (global/default) Config.
	applyFlagOverrides(&mc.Config,
		seenFlags,
		*listen, *admAddr, *strategyFlag, *hashKeyFlag, *backpressureFlag,
		*maxPerProxy, *maxClients, *logLevel, *logFormat,
		*udpEnabled, *otelEnabled, *otelEndpoint, *otelInsecure,
	)

	// Merge CLI-supplied upstreams into the global config (used as default for
	// single-group mode or as a fallback for groups that don't define upstreams).
	for _, spec := range upstreamsFlag {
		u, err := config.ParseUpstream(spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid upstream %q: %v\n", spec, err)
			os.Exit(2)
		}
		mc.Config.Upstreams = append(mc.Config.Upstreams, config.UpstreamSpec{
			Host:     u.Host,
			Port:     u.Port,
			Username: u.Username,
			Password: u.Password,
			Weight:   u.Weight,
			Priority: u.Priority,
		})
	}

	// Ensure at least one group has upstreams.
	effs := mc.EffectiveGroups()
	for _, eff := range effs {
		if len(eff.Upstreams) == 0 {
			fmt.Fprintln(os.Stderr, "error: at least one upstream is required (config or -upstream)")
			flag.Usage()
			os.Exit(2)
		}
	}

	if err := mc.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(2)
	}

	// --- Logger ---
	log := logging.New(mc.Config.LogLevel, mc.Config.LogFormat, "socks5lb", version)
	log.Info("starting",
		"version", version, "commit", commit, "build_date", buildDate,
		"groups", len(effs),
		"otel_enabled", mc.Config.OTel.Enabled,
	)

	// --- Metrics ---
	reg := prometheus.NewRegistry()
	if err := metrics.RegisterRuntime(reg); err != nil {
		fmt.Fprintf(os.Stderr, "metrics runtime registration failed: %v\n", err)
		os.Exit(2)
	}
	if err := metrics.RegisterBuildInfo(reg, version, commit, buildDate); err != nil {
		fmt.Fprintf(os.Stderr, "metrics build_info registration failed: %v\n", err)
		os.Exit(2)
	}

	// --- OTel ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prov, err := telemetry.Init(ctx, mc.Config.OTel, version, commit)
	if err != nil {
		log.Error("otel_init_failed", "err", err.Error())
		os.Exit(2)
	}
	defer func() {
		sctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cc()
		if err := prov.Shutdown(sctx); err != nil {
			log.Error("otel_shutdown_failed", "err", err.Error())
		}
	}()

	// --- Manager ---
	mgr, err := balancer.NewManager(mc, *configPath, log, reg, prov.Tracer, version, commit, buildDate)
	if err != nil {
		log.Error("manager_init_failed", "err", err.Error())
		os.Exit(2)
	}

	// --- Admin ---
	var adm *admin.Server
	if mc.Config.AdminAddr != "" {
		adm, err = admin.New(mc.Config.AdminAddr, mgr, mgr, reg, log, version, commit, buildDate,
			admin.Options{
				BearerToken:      mc.Config.AdminToken,
				EnablePprof:      mc.Config.AdminPprof,
				GroupProvider:    mgr,
				GroupReloader:    mgr,
				StateProvider:    mgr,
				SessionsProvider: mgr,
				DrainController:  mgr,
			})
		if err != nil {
			log.Error("admin_init_failed", "err", err.Error())
			os.Exit(2)
		}
		adm.Start()
	}

	// --- Signals / lifecycle ---
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	serveErr := make(chan error, 1)
	go func() { serveErr <- mgr.Start(ctx) }()

	for {
		select {
		case sig := <-sigCh:
			switch sig {
			case syscall.SIGHUP:
				log.Info("signal_received_reload")
				if err := mgr.Reload(); err != nil {
					log.Error("reload_failed", "err", err.Error())
				}
				continue
			default:
				log.Info("signal_received", "signal", sig.String())
				mgr.Shutdown()
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
}

// applyFlagOverrides merges non-zero flag values over cfg.
// seen tracks which flags were explicitly provided by the user; boolean
// overrides are only applied when the corresponding flag was seen.
func applyFlagOverrides(
	cfg *config.Config,
	seen map[string]bool,
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
	if seen["udp"] {
		cfg.UDPEnabled = udpEnabled
	}
	if seen["otel"] {
		cfg.OTel.Enabled = otelEnabled
	}
	if otelEndpoint != "" {
		cfg.OTel.Endpoint = otelEndpoint
		cfg.OTel.Enabled = true
	}
	if seen["otel-insecure"] {
		cfg.OTel.Insecure = otelInsecure
	}
}
