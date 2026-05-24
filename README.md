# s5lb

A SOCKS5 load balancer with pluggable strategies, configurable backpressure, UDP ASSOCIATE, OpenTelemetry tracing, Prometheus metrics, two-phase graceful drain, and SIGHUP hot reload.

> **Status:** feature-complete PoC+. This is the full feature surface for a production LB, but has **not** undergone the hardening pass (fuzzing, chaos testing, soak tests, security audit). Do not run in production as-is.

## Features

- **9 balancing strategies**: least-active, round-robin, weighted-round-robin, random, weighted-random, p2c, least-latency, consistent-hash (HRW), priority-failover
- **4 backpressure strategies**: reject, wait, drop-oldest, drop-lowest-priority
- **SOCKS5 CONNECT + UDP_ASSOCIATE** (RFC 1928/1929); BIND is explicitly unsupported
- **Per-upstream concurrency cap** + FIFO waiter queue + global admission gate
- **Active health probing** with sliding-window circuit breaker
- **EWMA latency** per upstream (feeds least-latency selector)
- **`splice(2)` zero-copy** TCP tunneling on Linux (when idle-timeout disabled)
- **OpenTelemetry** tracing via OTLP/gRPC (opt-in)
- **Prometheus metrics**, liveness/readiness, private pprof mux
- **YAML config** + CLI flag overrides
- **SIGHUP hot reload** of upstreams, backpressure, timeouts, log level
- **Two-phase graceful drain**: soft (wait for sessions) + hard (force close)

## Layout

```
cmd/s5lb/          # entrypoint
internal/
  admin/               # /metrics /healthz /readyz /version /admin/reload /debug/pprof
  admission/           # global admission gate + backpressure strategies + session tracker
  balancer/            # accept loop, queue, health, CONNECT + UDP sessions, reload
  config/              # YAML loader, defaults, validation, upstream spec parser
  logging/             # slog setup
  metrics/             # Prometheus collectors
  socks5/              # wire protocol (TCP + UDP header codec) + pipe
  strategy/            # pluggable LB strategies (pure functions of snapshots)
  telemetry/           # OpenTelemetry setup (opt-in)
  upstream/            # Upstream model + State (self-locking) + EWMA
```

## Build

```bash
make build
./bin/s5lb -version
```

## Quick start

```bash
./bin/s5lb -config config.example.yaml -log-level debug
# or flags-only:
./bin/s5lb \
  -strategy p2c \
  -backpressure drop-oldest \
  -upstream '10.0.0.1:1080#w=3,p=0' \
  -upstream '10.0.0.2:1080#w=1,p=0'
```

## Strategies

| Name                       | Description                                 |
| -------------------------- | ------------------------------------------- |
| `least-active` _(default)_ | Fewest active connections; random tie-break |
| `round-robin`              | Strict rotation                             |
| `weighted-round-robin`     | Smooth WRR (nginx algorithm)                |
| `random`                   | Uniform random                              |
| `weighted-random`          | Proportional to `weight`                    |
| `p2c`                      | Power of two choices                        |
| `least-latency`            | Lowest EWMA dial+handshake                  |
| `consistent-hash`          | Rendezvous hashing (sticky)                 |
| `priority-failover`        | Ordered by `priority`                       |

## Backpressure strategies

| Name                    | Behavior on `max_clients` saturation                    |
| ----------------------- | ------------------------------------------------------- |
| `reject` _(default)_    | Fast-fail with `RepGeneralFailure`                      |
| `wait`                  | Block up to `admission_wait_timeout`                    |
| `drop-oldest`           | Evict the oldest in-flight session                      |
| `drop-lowest-priority`  | Evict oldest session on the lowest-priority upstream   |

Eviction closes the victim's client socket; the session goroutine unwinds normally.

## Upstream spec forms (flag)

```
host:port
user:pass@host:port
socks5://user:pass@host:port?weight=N&priority=N&id=foo
host:port#w=N,p=N,id=foo
```

Or define them in YAML — see `config.example.yaml`.

## Observability

Admin port exposes:

| Path             | Purpose                              |
| ---------------- | ------------------------------------ |
| `/metrics`       | Prometheus scrape                    |
| `/healthz`       | Liveness (always OK while alive)     |
| `/readyz`        | 200 iff ≥1 upstream healthy          |
| `/version`       | Build info JSON                      |
| `/admin/reload`  | `POST` -> reload from config file    |
| `/debug/pprof/*` | Profiling (private mux, not default) |

### OpenTelemetry

Set `otel.enabled: true` and `otel.endpoint: "otel-collector:4317"` in config (or `-otel-endpoint`). Spans emitted per session:

```
socks5.session
├── socks5.greeting
├── socks5.request
├── balancer.acquire
├── upstream.dial
├── upstream.handshake
├── upstream.connect     (for CONNECT)
├── upstream.udp_associate  (for UDP ASSOCIATE)
└── tunnel.pipe          (TCP only)
```

### Useful PromQL

```promql
# Spread across upstreams
sum by (upstream) (rate(s5lb_upstream_selected_total[5m]))

# p99 dial latency
histogram_quantile(0.99,
  sum by (le, upstream) (rate(s5lb_upstream_dial_seconds_bucket[5m])))

# Queue pressure
s5lb_queue_depth
histogram_quantile(0.99, sum by (le) (rate(s5lb_queue_wait_seconds_bucket[5m])))

# Backpressure evictions per second
rate(s5lb_backpressure_evictions_total[1m])

# UDP drop rate by reason
sum by (reason) (rate(s5lb_udp_dropped_total[5m]))
```

## Lifecycle

- **`SIGTERM` / `SIGINT`** — two-phase drain:
    1. Stop accepting, cancel context.
    2. Wait up to `drain_soft_timeout` for sessions to finish.
    3. Force-close any survivors; wait up to `drain_hard_timeout`.
- **`SIGHUP`** — reload config file. Reloadable: upstreams, backpressure, timeouts, log level. Not reloadable: listen addr, admin addr, strategy, hash_key, max_clients, OTel (documented as warnings in logs).
- **`POST /admin/reload`** — same as SIGHUP.

## Kubernetes probes

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
  periodSeconds: 5
```

## Testing

```bash
make test
```

The included suite covers: SOCKS5 protocol greeting/request parsing, UDP header codec round-trip, every strategy's determinism + eligibility filtering, admission tracker ordering, and queue FIFO behavior.

## Known limitations (intentional for this iteration)

- **No frontend auth / ACLs.** Anyone reachable at `listen` can use it. Plan for hardening pass.
- **No per-IP rate limiting.**
- **No TLS to upstreams.**
- **No BIND command.** Responds with `RepCommandNotSupported`.
- **Single-instance.** No `SO_REUSEPORT`, no clustering.
- **UDP: no fragmentation.** Datagrams with `FRAG != 0` are dropped.

## License

MIT
