// Package transport provides pluggable byte-stream transports for upstream
// SOCKS5 connections.
//
// The balancer only needs a net.Conn. Direct TCP, Kubernetes exec, WebSocket
// exec, or future multiplexed transports can all implement Dialer.
package transport

import (
	"context"
	"net"
)

// Dialer opens a full-duplex byte stream to address.
//
// For direct TCP transports, address is host:port.
//
// For Kubernetes exec transports, address is still host:port, but the TCP
// connection is opened by a command running inside the selected pod/container.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}
