package socks5

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"time"
)

// halfCloser is implemented by *net.TCPConn and any net.Conn that supports
// graceful half-close. When unavailable we fall back to a full Close.
type halfCloser interface {
	CloseWrite() error
}

// TuneSocket enables TCP_NODELAY and (optionally) keepalive on a *net.TCPConn.
// Errors are logged at debug level — they are non-fatal but worth knowing.
func TuneSocket(conn net.Conn, keepalive bool) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if err := tcp.SetNoDelay(true); err != nil {
		slog.Default().Debug("tcp_set_nodelay_failed", "err", err.Error())
	}
	if keepalive {
		if err := tcp.SetKeepAlive(true); err != nil {
			slog.Default().Debug("tcp_set_keepalive_failed", "err", err.Error())
		}
		if err := tcp.SetKeepAlivePeriod(30 * time.Second); err != nil {
			slog.Default().Debug("tcp_set_keepalive_period_failed", "err", err.Error())
		}
	}
}

// Pipe bridges client<->upstream bidirectionally over TCP.
// Returns (bytesClientToUpstream, bytesUpstreamToClient).
//
// On Linux with idle==0, io.Copy over *net.TCPConn engages splice(2) for
// kernel-level zero-copy. Setting idle>0 forces the poller path but enables
// per-direction idle timeouts.
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

	select {
	case r := <-upCh:
		if hc, ok := upstream.(halfCloser); ok {
			_ = hc.CloseWrite()
		} else {
			_ = upstream.Close()
		}
		up = r.n
		down = (<-downCh).n
	case r := <-downCh:
		if hc, ok := client.(halfCloser); ok {
			_ = hc.CloseWrite()
		} else {
			_ = client.Close()
		}
		down = r.n
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
			// Loop to handle short writes (io.ErrShortWrite).
			written := 0
			for written < n {
				nw, werr := dst.Write(buf[written:n])
				written += nw
				total += int64(nw)
				if werr != nil {
					return total, werr
				}
				// Treat zero-byte successful write as a short write to avoid
				// spinning forever on custom net.Conn implementations.
				if nw == 0 {
					return total, io.ErrShortWrite
				}
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
