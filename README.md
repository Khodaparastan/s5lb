# s5lb

A SOCKS5 load balancer written in Go with pluggable balancing strategies, configurable backpressure, UDP ASSOCIATE, OpenTelemetry tracing, Prometheus metrics, two-phase graceful drain, SIGHUP hot reload, multi-group mode, and pluggable upstream transports (direct TCP and Kubernetes pod exec).

## Features

- **9 balancing strategies**: least-active, round-robin, weighted-round-robin, random, weighted-random, p2c, least-latency, consistent-hash (HRW), priority-failover
- **4 backpressure strategies**: reject, wait, drop-oldest, drop-lowest-priority
- **SOCKS5 CONNECT + UDP_ASSOCIATE** (RFC 1928/1929); BIND explicitly unsupported
- **Per-upstream concurrency cap** + FIFO waiter queue + global admission gate
- **Active health probing** with sliding-window circuit breaker
- **EWMA latency** per upstream (feeds least-latency selector)
- **`splice(2)` zero-copy** TCP tunneling on Linux (when idle-timeout is disabled)
- **Multi-group mode**: run independent SOCKS5 listener pools from a single process, each with its own strategy, upstreams, and transport
- **Pluggable upstream transports**: direct TCP or Kubernetes pod exec (SPDY, WebSocket, or raw WSS); includes a `kube-socks-relay` sidecar binary
- **OpenTelemetry** tracing via OTLP/gRPC (opt-in)
- **Prometheus metrics**, liveness/readiness probes, optional pprof
- **Admin UI**: single-page dashboard with live SSE state stream, upstream drain control, and session viewer
- **Admin REST API**: multi-group endpoints, sessions, upstream drain, config snapshot, reload
- **YAML config** + CLI flag overrides
- **SIGHUP hot reload** of upstreams, backpressure, timeouts, log level
- **Two-phase graceful drain**: soft (wait for sessions) + hard (force close)

## Layout

```
cmd/
  s5lb/              # main entrypoint
  kube-socks-relay/  # sidecar: connects stdin/stdout to host:port (used by kubernetes-exec transport)
internal/
  admin/             # HTTP admin server: UI, /metrics, /healthz, /readyz, /version, REST API, pprof
  admission/         # global admission gate + backpressure strategies + session tracker
  balancer/          # accept loop, queue, health probing, CONNECT + UDP sessions, reload
  config/            # YAML loader, defaults, validation, upstream spec parser
  logging/           # slog setup
  metrics/           # Prometheus collectors
  socks5/            # wire protocol (TCP + UDP header codec) + pipe
  strategy/          # pluggable LB strategies (pure functions of snapshots)
  telemetry/         # OpenTelemetry setup (opt-in)
  transport/         # upstream dialers: direct TCP, kubernetes-exec (SPDY/WS/raw-WSS)
  upstream/          # Upstream model + State (self-locking) + EWMA
```

## Build

```bash
make build          # produces ./bin/s5lb
./bin/s5lb -version
```

Build both binaries:
```bash
go build ./cmd/s5lb
go build ./cmd/kube-socks-relay
```

Cross-platform release artifacts:
```bash
make release        # linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64 → ./dist/
```

## Quick start

```bash
# Config file
./bin/s5lb -config config.example.yaml -log-level debug

# Flags only
./bin/s5lb \
  -strategy p2c \
  -backpressure drop-oldest \
  -upstream '10.0.0.1:1080#w=3,p=0' \
  -upstream '10.0.0.2:1080#w=1,p=0'
```

## CLI flags

| Flag | Default | Description |
| --- | --- | --- |
| `-config` | | Path to YAML config file; flags override |
| `-listen` | `127.0.0.1:1080` | Frontend listen address |
| `-admin-addr` | `127.0.0.1:9090` | Admin HTTP listen address |
| `-upstream` | | Upstream spec (repeatable) |
| `-strategy` | `least-active` | Balancing strategy |
| `-hash-key` | `client-ip` | `client-ip \| destination \| destination-host` |
| `-backpressure` | `reject` | Backpressure strategy |
| `-max-per-proxy` | `100` | Max concurrent connections per upstream |
| `-max-clients` | `4096` | Global max concurrent clients |
| `-udp` | `true` | Enable UDP_ASSOCIATE |
| `-otel` | `false` | Enable OpenTelemetry tracing |
| `-otel-endpoint` | | OTLP gRPC endpoint (e.g. `otel-collector:4317`) |
| `-otel-insecure` | `true` | Disable TLS to OTLP endpoint |
| `-log-level` | `info` | `debug \| info \| warn \| error` |
| `-log-format` | `json` | `json \| text` |
| `-version` | | Print version and exit |
| `-list-strategies` | | List available strategies and exit |

## Strategies

| Name                       | Description                                 |
| -------------------------- | ------------------------------------------- |
| `least-active` _(default)_ | Fewest active connections; random tie-break |
| `round-robin`              | Strict rotation                             |
| `weighted-round-robin`     | Smooth WRR (nginx algorithm)                |
| `random`                   | Uniform random                              |
| `weighted-random`          | Proportional to `weight`                    |
| `p2c`                      | Power of two choices                        |
| `least-latency`            | Lowest EWMA dial+handshake latency          |
| `consistent-hash`          | Rendezvous hashing (HRW), sticky by `hash_key` |
| `priority-failover`        | Ordered by `priority`; failover on unhealthy |

## Backpressure strategies

| Name                       | Behavior when `max_clients` is saturated                |
| -------------------------- | ------------------------------------------------------- |
| `reject` _(default)_       | Fast-fail with `RepGeneralFailure`                      |
| `wait`                     | Block up to `admission_wait_timeout`                    |
| `drop-oldest`              | Evict the oldest in-flight session                      |
| `drop-lowest-priority`     | Evict oldest session on the lowest-priority upstream    |

Eviction closes the victim's client socket; the session goroutine unwinds normally.

## Upstream spec forms (flag)

```
host:port
user:pass@host:port
socks5://user:pass@host:port?weight=N&priority=N&id=foo
host:port#w=N,p=N,id=foo
```

Or define them in YAML — see `config.example.yaml`.

## Config reference

All fields with defaults (single-group mode):

```yaml
listen: "127.0.0.1:1080"
admin:  "127.0.0.1:9090"

max_per_proxy: 100
max_clients:   4096

health_interval:    20s
retry_backoff:      30s
connect_timeout:    5s
handshake_timeout:  10s
queue_wait_timeout: 10s

failure_threshold: 5
failure_window:    30s

idle_timeout: 0s   # 0 enables splice(2) fast path on Linux

drain_soft_timeout: 20s
drain_hard_timeout: 10s

tcp_keepalive: true

strategy: "least-active"
hash_key: "client-ip"   # client-ip | destination | destination-host

backpressure: "reject"
admission_wait_timeout: 2s

udp_enabled:      true
udp_bind:         ""
udp_idle_timeout: 60s

# admin_token: ""   # required when admin is not on loopback
# admin_pprof: false

transport:
  type: direct   # direct | kubernetes-exec

otel:
  enabled:      false
  endpoint:     ""
  insecure:     true
  service_name: "s5lb"
  sample_ratio: 1.0
  headers: {}

log_level:  "info"
log_format: "json"

upstreams:
  - host: "10.0.0.1"
    port: 1080
    username: "user"
    password: "pass"
    weight:   3
    priority: 0
```

### Reloadable fields (SIGHUP / `POST /admin/reload`)

Upstreams, backpressure, timeouts, and log level are reloaded at runtime.  
Not reloadable (documented as warnings in logs): listen addr, admin addr, strategy, hash_key, max_clients, OTel.

## Multi-group mode

When `groups:` is present in config, each entry is an independent SOCKS5 listener pool with its own upstreams, strategy, and transport. Global settings (OTel, admin, log) are shared.

```yaml
groups:
  - name: web
    listen: "0.0.0.0:1080"
    strategy: least-active
    upstreams:
      - { host: proxy-web-1.internal, port: 1080, weight: 3 }
      - { host: proxy-web-2.internal, port: 1080, weight: 1 }

  - name: db
    listen: "0.0.0.0:1081"
    strategy: round-robin
    connect_timeout: 3s
    upstreams:
      - { host: proxy-db-1.internal, port: 1080 }

  - name: k8s
    listen: "0.0.0.0:1082"
    strategy: least-latency
    transport:
      type: kubernetes-exec
      kubernetes:
        namespace: relay
        pod: relay-0
        container: relay
        command: ["/usr/local/bin/kube-socks-relay", "tcp", "{{host}}", "{{port}}"]
    upstreams:
      - { host: internal-svc.cluster.local, port: 8080 }
```

## Upstream transports

### `direct` (default)

Plain TCP dial. Supports keepalive and configurable connect timeout.

### `kubernetes-exec`

Opens upstream byte streams through Kubernetes pod exec. The exec command must bridge its stdin/stdout to the target host:port. The included `kube-socks-relay` binary is designed for this role.

```yaml
transport:
  type: kubernetes-exec
  kubernetes:
    kubeconfig: ""         # empty = in-cluster or $KUBECONFIG
    context:    ""
    namespace:  relay
    pod:        relay-0
    container:  relay
    mode:       websocket-preferred  # spdy | websocket | websocket-preferred | raw-wss
    command:    ["/usr/local/bin/kube-socks-relay", "tcp", "{{host}}", "{{port}}"]
    # wss_url: "wss://api.example.com/api/v1/namespaces/ns/pods/pod/exec?container=relay"
    # bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    # ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
```

Command template placeholders: `{{host}}`, `{{port}}`, `{{address}}`.

#### `kube-socks-relay`

A minimal sidecar binary that connects stdin/stdout to a TCP address. Deploy it inside the relay pod and reference it in `transport.kubernetes.command`.

```bash
kube-socks-relay tcp <host> <port>
```

## Observability

### Admin endpoints

| Path                                  | Method | Purpose                                       |
| ------------------------------------- | ------ | --------------------------------------------- |
| `/metrics`                            | GET    | Prometheus scrape                             |
| `/healthz`                            | GET    | Liveness (always OK while alive)              |
| `/readyz`                             | GET    | 200 iff ≥1 upstream healthy                  |
| `/version`                            | GET    | Build info JSON                               |
| `/admin/reload`                       | POST   | Reload from config file                       |
| `/admin/api/groups`                   | GET    | JSON array of all group summaries             |
| `/admin/api/groups/{name}`            | GET    | JSON detail for one group                     |
| `/admin/api/groups/{name}/reload`     | POST   | Reload a single group                         |
| `/admin/api/state`                    | GET    | Flat single-group state snapshot              |
| `/admin/api/events`                   | GET    | SSE stream of state updates (2 s interval)    |
| `/admin/api/sessions`                 | GET    | Active session list                           |
| `/admin/api/config`                   | GET    | Running config snapshot (JSON)                |
| `/admin/api/reload`                   | POST   | Reload (alias for UI)                         |
| `/admin/api/upstreams/{id}/drain`     | POST   | Set/clear drain flag (`?enabled=true\|false`) |
| `/debug/pprof/*`                      | GET    | Profiling (opt-in via `admin_pprof: true`)    |

**Admin UI**: browsing to `http://<admin-addr>/` serves a single-page dashboard with live upstream status, session viewer, and reload/drain controls.

**Security**: when `admin` is not a loopback address, `admin_token` is required. Requests must include `Authorization: Bearer <token>`.

### OpenTelemetry

Set `otel.enabled: true` and `otel.endpoint: "otel-collector:4317"`. Spans emitted per session:

```
socks5.session
├── socks5.greeting
├── socks5.request
├── balancer.acquire
├── upstream.dial
├── upstream.handshake
├── upstream.connect        (CONNECT)
├── upstream.udp_associate  (UDP ASSOCIATE)
└── tunnel.pipe             (TCP only)
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
    3. Force-close survivors; wait up to `drain_hard_timeout`.
- **`SIGHUP`** — reload config file (upstreams, backpressure, timeouts, log level).
- **`POST /admin/reload`** — same as SIGHUP.
- **`POST /admin/api/groups/{name}/reload`** — reload a single group.
- **`POST /admin/api/upstreams/{id}/drain?enabled=true`** — drain a single upstream without restart.

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
make test        # full suite with race detector
make check       # fmt + vet + test
```

The suite covers: SOCKS5 protocol greeting/request parsing, UDP header codec round-trip, every strategy's determinism and eligibility filtering, admission tracker ordering, queue FIFO behavior, config loading, and multi-group config merging.

## Known limitations

- **No frontend auth / ACLs.** Anyone reachable at `listen` can use it.
- **No per-IP rate limiting.**
- **No TLS to upstreams** (direct transport).
- **No BIND command.** Responds with `RepCommandNotSupported`.
- **Single-instance.** No `SO_REUSEPORT`, no clustering.
- **UDP: no fragmentation.** Datagrams with `FRAG != 0` are dropped.

## License

MIT
