package balancer

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"time"

	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// newSessionID returns a short hex identifier for log correlation.
func newSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// handleClient drives a single client session end-to-end: greeting, request
// parsing, upstream acquisition, dial/handshake, CONNECT relay, and tunneling.
func (lb *LoadBalancer) handleClient(client net.Conn) {
	sid := newSessionID()
	log := lb.log.With(
		"session_id", sid,
		"client", client.RemoteAddr().String(),
	)

	lb.track(client)
	defer lb.untrack(client)
	defer client.Close()

	socks5.TuneSocket(client, lb.cfg.TCPKeepAlive)
	br := bufio.NewReaderSize(client, 4096)

	log.Debug("accepted")

	// 1. Greeting.
	_ = client.SetDeadline(time.Now().Add(lb.cfg.HandshakeTimeout))
	if err := socks5.ReadGreeting(client, br); err != nil {
		lb.metrics.RejectedTotal.WithLabelValues("greet_failed").Inc()
		log.Warn("greet_failed", "err", err.Error())
		return
	}

	// 2. CONNECT request.
	req, replyWritten, err := socks5.ReadRequest(client, br)
	if err != nil {
		atyp := "unknown"
		if req != nil {
			atyp = socks5.AtypLabel(req.Atyp)
		}
		lb.metrics.SocksRequest.WithLabelValues(atyp, "parse_err").Inc()
		if replyWritten != 0 {
			lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(replyWritten)).Inc()
		}
		log.Warn("request_failed", "err", err.Error())
		return
	}
	log = log.With(
		"dst", req.DstLabel,
		"dst_port", req.Port,
		"atyp", socks5.AtypLabel(req.Atyp),
	)
	log.Debug("request_parsed")
	lb.metrics.SocksRequest.WithLabelValues(socks5.AtypLabel(req.Atyp), "ok").Inc()

	// Clear handshake deadline before potentially long queue wait.
	_ = client.SetDeadline(time.Time{})

	// 3. Acquire upstream slot.
	sc := strategy.SelectCtx{
		ClientAddr: client.RemoteAddr(),
		DstHost:    req.DstLabel,
		DstPort:    req.Port,
		HashKey:    lb.cfg.HashKey,
	}
	up := lb.acquireSlot(lb.ctx, sc, log)
	if up == nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		return
	}
	defer lb.releaseSlot(up)
	log = log.With("upstream", up.Addr())

	// 4. Dial + handshake upstream.
	upConn, err := lb.dialAndHandshakeUpstream(up, log)
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		return
	}
	lb.track(upConn)
	defer lb.untrack(upConn)
	defer upConn.Close()

	// 5. CONNECT upstream.
	connStart := time.Now()
	rep, err := socks5.ClientConnect(upConn, req.Atyp, req.RawAddr, req.Port)
	if err != nil {
		lb.recordFailure(up, "connect", err.Error())
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("upstream_connect_failed",
			"err", err.Error(),
			"elapsed_ms", time.Since(connStart).Milliseconds())
		return
	}
	log.Debug("upstream_connect",
		"reply", socks5.ReplyLabel(rep),
		"elapsed_ms", time.Since(connStart).Milliseconds())

	_, _ = client.Write(socks5.ReplyBytes(rep))
	lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(rep)).Inc()
	lb.recordSuccess(up)

	if rep != socks5.RepSuccess {
		return
	}

	// 6. Tunnel.
	_ = upConn.SetDeadline(time.Time{})
	lb.metrics.ActiveSessions.Inc()
	sessStart := time.Now()
	log.Info("session_start")

	bytesUp, bytesDown := socks5.Pipe(client, upConn, lb.cfg.IdleTimeout)

	lb.metrics.ActiveSessions.Dec()
	lb.metrics.SessionBytes.WithLabelValues("upstream").Add(float64(bytesUp))
	lb.metrics.SessionBytes.WithLabelValues("downstream").Add(float64(bytesDown))
	lb.metrics.SessionDuration.Observe(time.Since(sessStart).Seconds())

	log.Info("session_end",
		"duration_ms", time.Since(sessStart).Milliseconds(),
		"bytes_up", bytesUp,
		"bytes_down", bytesDown,
	)
}

// dialAndHandshakeUpstream performs TCP dial + SOCKS5 client handshake,
// recording latency/failure metrics and updating the upstream's EWMA.
func (lb *LoadBalancer) dialAndHandshakeUpstream(u *upstream.Upstream, log *slog.Logger) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(lb.ctx, lb.cfg.ConnectTimeout)
	defer cancel()

	dialStart := time.Now()
	d := net.Dialer{Timeout: lb.cfg.ConnectTimeout}
	c, err := d.DialContext(ctx, "tcp", u.Addr())
	dialDur := time.Since(dialStart)
	lb.metrics.UpDialSec.WithLabelValues(u.Addr()).Observe(dialDur.Seconds())
	if err != nil {
		lb.recordFailure(u, "dial", err.Error())
		log.Warn("upstream_dial_failed",
			"err", err.Error(),
			"elapsed_ms", dialDur.Milliseconds())
		return nil, err
	}
	log.Debug("upstream_dial_ok", "elapsed_ms", dialDur.Milliseconds())

	socks5.TuneSocket(c, lb.cfg.TCPKeepAlive)
	_ = c.SetDeadline(time.Now().Add(lb.cfg.HandshakeTimeout))

	hsStart := time.Now()
	if err := socks5.ClientHandshake(c, u.Username, u.Password); err != nil {
		hsDur := time.Since(hsStart)
		lb.metrics.UpHandshake.WithLabelValues(u.Addr()).Observe(hsDur.Seconds())
		lb.recordFailure(u, "handshake", err.Error())
		log.Warn("upstream_handshake_failed",
			"err", err.Error(),
			"elapsed_ms", hsDur.Milliseconds())
		_ = c.Close()
		return nil, err
	}
	hsDur := time.Since(hsStart)
	lb.metrics.UpHandshake.WithLabelValues(u.Addr()).Observe(hsDur.Seconds())

	// Record combined (dial+handshake) latency into the EWMA — used by the
	// least-latency selector.
	u.ObserveLatency(dialDur + hsDur)

	log.Debug("upstream_handshake_ok", "elapsed_ms", hsDur.Milliseconds())
	return c, nil
}
