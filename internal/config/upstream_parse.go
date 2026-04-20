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
		applyShorthand(u, spec[hash+1:])
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
		if colon := strings.Index(creds, ":"); colon >= 0 {
			u.Username, u.Password = creds[:colon], creds[colon+1:]
		} else {
			u.Username = creds
		}
	}
	host, portStr, err := net.SplitHostPort(hp)
	if err != nil {
		return nil, fmt.Errorf("split host:port %q: %w", hp, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("port %q: %w", portStr, err)
	}
	u.Host, u.Port = host, port
	u.NormalizeID()
	return u, nil
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
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	u.Host, u.Port = host, port
	applyQuery(u, p.Query())
	return nil
}

func applyQuery(u *upstream.Upstream, q url.Values) {
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
	if v := q.Get("id"); v != "" {
		u.ID = v
	}
}

func applyShorthand(u *upstream.Upstream, s string) {
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
		case "id":
			u.ID = val
		}
	}
}
