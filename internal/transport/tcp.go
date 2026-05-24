package transport

import (
	"context"
	"net"
	"time"
)

// TCPDialer is the default direct upstream dialer.
type TCPDialer struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

// DialContext opens a direct network connection.
func (d TCPDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	nd := net.Dialer{
		Timeout:   d.Timeout,
		KeepAlive: d.KeepAlive,
	}

	return nd.DialContext(ctx, network, address)
}
