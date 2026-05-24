package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// ParseUpstream accepts any of:
//
//	host:port
//	user:pass@host:port
//	socks5://user:pass@host:port?weight=N&priority=N
//	host:port#w=N,p=N,id=foo
func ParseUpstream(spec string) (*upstream.Upstream, error) {
	u := &upstream.Upstream{Weight: 1, Priority: 0, State: upstream.NewState()}

	// Trailing "#k=v,k=v" shorthand.
	if hash := strings.Index(spec, "#"); hash >= 0 {
		if err := applyShorthand(u, spec[hash+1:]); err != nil {
			return nil, fmt.Errorf("shorthand: %w", err)
		}
		spec = spec[:hash]
	}

	if strings.Contains(spec, "://") {
		if err := parseURLForm(u, spec); err != nil {
			return nil, err
		}
		u.NormalizeID()
		return u, nil
	}

	hp := spec
	if at := strings.LastIndex(spec, "@"); at >= 0 {
		creds := spec[:at]
		hp = spec[at+1:]
		if before, after, ok := strings.Cut(creds, ":"); ok {
			u.Username, u.Password = before, after
		} else {
			u.Username = creds
		}
	}
	host, portStr, err := net.SplitHostPort(hp)
	if err != nil {
		return nil, fmt.Errorf("split host:port %q: %w", hp, err)
	}
	port, err := parsePort(portStr)
	if err != nil {
		return nil, err
	}
	u.Host, u.Port = host, port
	u.NormalizeID()
	return u, nil
}

// parsePort parses a port string and validates it is in [1,65535].
func parsePort(portStr string) (int, error) {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("port %q: %w", portStr, err)
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %d out of range [1,65535]", port)
	}
	return port, nil
}

func parseURLForm(u *upstream.Upstream, spec string) error {
	p, err := url.Parse(spec)
	if err != nil {
		return err
	}
	if p.Scheme != "socks5" && p.Scheme != "socks5h" {
		return fmt.Errorf("unsupported scheme %q", p.Scheme)
	}
	if p.User != nil {
		u.Username = p.User.Username()
		if pw, ok := p.User.Password(); ok {
			u.Password = pw
		}
	}
	host, portStr, err := net.SplitHostPort(p.Host)
	if err != nil {
		return err
	}
	port, err := parsePort(portStr)
	if err != nil {
		return err
	}
	u.Host, u.Port = host, port
	return applyQuery(u, p.Query())
}

func applyQuery(u *upstream.Upstream, q url.Values) error {
	if v := q.Get("weight"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid weight %q: %w", v, err)
		}
		if n <= 0 {
			return fmt.Errorf("weight must be > 0, got %d", n)
		}
		u.Weight = n
	}
	if v := q.Get("priority"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid priority %q: %w", v, err)
		}
		u.Priority = n
	}
	if v := q.Get("id"); v != "" {
		u.ID = v
	}
	return nil
}

func applyShorthand(u *upstream.Upstream, s string) error {
	for kv := range strings.SplitSeq(s, ",") {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid shorthand pair %q", kv)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "w", "weight":
			n, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("invalid weight %q: %w", val, err)
			}
			if n <= 0 {
				return fmt.Errorf("weight must be > 0, got %d", n)
			}
			u.Weight = n
		case "p", "priority":
			n, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("invalid priority %q: %w", val, err)
			}
			u.Priority = n
		case "id":
			u.ID = val
		default:
			return fmt.Errorf("unknown shorthand key %q", key)
		}
	}
	return nil
}
