package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

type Config struct {
	ListenAddr string
	AdminAddr  string

	MaxPerProxy int
	MaxClients  int

	HealthInterval   time.Duration
	RetryBackoff     time.Duration
	ConnectTimeout   time.Duration
	HandshakeTimeout time.Duration
	QueueWaitTimeout time.Duration
	FailureThreshold int
	FailureWindow    time.Duration
	IdleTimeout      time.Duration
	DrainTimeout     time.Duration
	TCPKeepAlive     bool

	Strategy string
	HashKey  strategy.HashKey

	LogLevel  slog.Level
	LogFormat string
}

func Defaults() Config {
	return Config{
		ListenAddr:       "127.0.0.1:1080",
		AdminAddr:        "127.0.0.1:9090",
		MaxPerProxy:      100,
		MaxClients:       4096,
		HealthInterval:   20 * time.Second,
		RetryBackoff:     30 * time.Second,
		ConnectTimeout:   5 * time.Second,
		HandshakeTimeout: 10 * time.Second,
		QueueWaitTimeout: 10 * time.Second,
		FailureThreshold: 5,
		FailureWindow:    30 * time.Second,
		IdleTimeout:      0,
		DrainTimeout:     10 * time.Second,
		TCPKeepAlive:     true,
		Strategy:         "least-active",
		HashKey:          strategy.HashClientIP,
		LogLevel:         slog.LevelInfo,
		LogFormat:        "json",
	}
}

// ParseUpstream accepts:
//
//	host:port
//	user:pass@host:port                    (password may contain ':' or '@')
//	socks5://user:pass@host:port?weight=N&priority=N
//	host:port#w=N,p=N
func ParseUpstream(spec string) (*upstream.Upstream, error) {
	u := &upstream.Upstream{Healthy: true, Weight: 1, Priority: 0}

	// Trailing "#k=v,k=v" shorthand.
	if hash := strings.Index(spec, "#"); hash >= 0 {
		applyShorthandParams(u, spec[hash+1:])
		spec = spec[:hash]
	}

	if strings.Contains(spec, "://") {
		p, err := url.Parse(spec)
		if err != nil {
			return nil, err
		}
		if p.Scheme != "socks5" && p.Scheme != "socks5h" {
			return nil, fmt.Errorf("unsupported scheme %q", p.Scheme)
		}
		if p.User != nil {
			u.Username = p.User.Username()
			if pw, ok := p.User.Password(); ok {
				u.Password = pw
			}
		}
		host, portStr, err := net.SplitHostPort(p.Host)
		if err != nil {
			return nil, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
		u.Host, u.Port = host, port
		applyURLParams(u, p.Query())
		return u, nil
	}

	hp := spec
	if at := strings.LastIndex(spec, "@"); at >= 0 {
		creds := spec[:at]
		hp = spec[at+1:]
		if colon := strings.Index(creds, ":"); colon >= 0 {
			u.Username, u.Password = creds[:colon], creds[colon+1:]
		} else {
			u.Username = creds
		}
	}
	host, portStr, err := net.SplitHostPort(hp)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	u.Host, u.Port = host, port
	return u, nil
}

func applyURLParams(u *upstream.Upstream, q url.Values) {
	if v := q.Get("weight"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			u.Weight = n
		}
	}
	if v := q.Get("priority"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			u.Priority = n
		}
	}
}

func applyShorthandParams(u *upstream.Upstream, s string) {
	for _, kv := range strings.Split(s, ",") {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "w", "weight":
			if n, err := strconv.Atoi(val); err == nil && n > 0 {
				u.Weight = n
			}
		case "p", "priority":
			if n, err := strconv.Atoi(val); err == nil {
				u.Priority = n
			}
		}
	}
}
