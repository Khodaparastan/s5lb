package socks5

import (
	"errors"
	"io"
	"net"
	"time"
)

// TuneSocket enables TCP_NODELAY and (optionally) keepalive on a net.Conn.
// Safe to call with non-TCP conns (no-op).
func TuneSocket(conn net.Conn, keepalive bool) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetNoDelay(true)
	if keepalive {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}
}

// Pipe bridges client<->upstream bidirectionally.
// Returns (bytesFromClientToUpstream, bytesFromUpstreamToClient).
//
// On Linux with idle==0, io.Copy over *net.TCPConn engages splice(2) for
// kernel-level zero-copy.  Setting idle>0 forces the poller path (no splice)
// but enables per-direction idle timeouts.
func Pipe(client, upstream net.Conn, idle time.Duration) (up, down int64) {
	type result struct {
		n   int64
		err error
	}
	upCh := make(chan result, 1)
	downCh := make(chan result, 1)

	go func() {
		n, err := copyDir(upstream, client, idle)
		upCh <- result{n, err}
	}()
	go func() {
		n, err := copyDir(client, upstream, idle)
		downCh <- result{n, err}
	}()

	var first result
	select {
	case first = <-upCh:
		if tc, ok := upstream.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		up = first.n
		down = (<-downCh).n
	case first = <-downCh:
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		down = first.n
		up = (<-upCh).n
	}
	return up, down
}

func copyDir(dst, src net.Conn, idle time.Duration) (int64, error) {
	if idle <= 0 {
		return io.Copy(dst, src)
	}
	buf := make([]byte, 32*1024)
	var total int64
	for {
		_ = src.SetReadDeadline(time.Now().Add(idle))
		n, rerr := src.Read(buf)
		if n > 0 {
			nw, werr := dst.Write(buf[:n])
			total += int64(nw)
			if werr != nil {
				return total, werr
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return total, nil
			}
			return total, rerr
		}
	}
}
