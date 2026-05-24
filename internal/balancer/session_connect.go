package balancer

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/khodaparastan/s5lb/internal/admission"
	"github.com/khodaparastan/s5lb/internal/config"
	"github.com/khodaparastan/s5lb/internal/socks5"
	"github.com/khodaparastan/s5lb/internal/strategy"
	"github.com/khodaparastan/s5lb/internal/transport"
	"github.com/khodaparastan/s5lb/internal/upstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// handleConnect implements CMD=CONNECT end-to-end.
func (lb *LoadBalancer) handleConnect(
	ctx context.Context,
	client net.Conn,
	_ *bufio.Reader,
	req *socks5.Request,
	sess *admission.Session,
	log *slog.Logger,
) {
	cfg := lb.Config()
	sc := strategy.SelectCtx{
		ClientAddr: client.RemoteAddr(),
		DstHost:    req.DstLabel,
		DstPort:    req.Port,
		HashKey:    cfg.HashKey,
	}

	// Acquire upstream slot.
	acqCtx, acqSpan := lb.tracer.Start(ctx, "balancer.acquire")
	up := lb.acquireSlot(acqCtx, sc, log)
	if up == nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		acqSpan.SetStatus(codes.Error, "no_upstream")
		acqSpan.End()
		return
	}
	defer lb.releaseSlot(up)
	acqSpan.SetAttributes(attribute.String("upstream.addr", up.Addr()))
	acqSpan.End()

	lb.tracker.Update(sess, up.ID, up.Priority)
	log = logWithUpstream(log, up.Addr())

	// Dial + upstream handshake.
	upConn, err := lb.dialAndHandshakeUpstream(ctx, up, log)
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		return
	}
	lb.track(upConn)
	defer lb.untrack(upConn)
	defer upConn.Close()

	// Upstream CONNECT.
	_, connSpan := lb.tracer.Start(ctx, "upstream.connect")
	connStart := time.Now()
	reply, err := socks5.ClientConnect(upConn, req.Atyp, req.RawAddr, req.Port)
	if err != nil {
		lb.recordFailure(up, "connect", err.Error())
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("upstream_connect_failed",
			"err", err.Error(),
			"elapsed_ms", time.Since(connStart).Milliseconds())
		connSpan.RecordError(err)
		connSpan.SetStatus(codes.Error, "connect_failed")
		connSpan.End()
		return
	}
	connSpan.SetAttributes(attribute.String("socks.reply", socks5.ReplyLabel(reply.Rep)))
	connSpan.End()
	log.Debug("upstream_connect_ok",
		"reply", socks5.ReplyLabel(reply.Rep),
		"elapsed_ms", time.Since(connStart).Milliseconds(),
	)

	_, _ = client.Write(socks5.ReplyBytes(reply.Rep))
	lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(reply.Rep)).Inc()
	lb.recordSuccess(up)
	if reply.Rep != socks5.RepSuccess {
		return
	}

	// Tunnel.
	_ = upConn.SetDeadline(time.Time{})
	lb.metrics.ActiveSessions.Inc()
	sessStart := time.Now()

	_, pipeSpan := lb.tracer.Start(ctx, "tunnel.pipe")
	log.Info("session_start")
	bytesUp, bytesDown := socks5.Pipe(client, upConn, cfg.IdleTimeout)
	pipeSpan.SetAttributes(
		attribute.Int64("bytes.up", bytesUp),
		attribute.Int64("bytes.down", bytesDown),
	)
	pipeSpan.End()

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

// dialAndHandshakeUpstream dials the upstream, performs SOCKS5 method
// negotiation + optional auth, and records metrics.
func (lb *LoadBalancer) dialAndHandshakeUpstream(
	ctx context.Context, u *upstream.Upstream, log *slog.Logger,
) (net.Conn, error) {
	cfg := lb.Config()

	dCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	_, dialSpan := lb.tracer.Start(ctx, "upstream.dial")
	dialStart := time.Now()
	d := lb.Dialer()
	if d == nil {
		d = transport.TCPDialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}
	}

	c, err := d.DialContext(dCtx, "tcp", u.Addr())
	dialDur := time.Since(dialStart)
	lb.metrics.UpDialSec.WithLabelValues(u.Addr()).Observe(dialDur.Seconds())
	if err != nil {
		lb.recordFailure(u, "dial", err.Error())
		log.Warn("upstream_dial_failed", "err", err.Error(), "elapsed_ms", dialDur.Milliseconds())
		dialSpan.RecordError(err)
		dialSpan.SetStatus(codes.Error, "dial_failed")
		dialSpan.End()
		return nil, err
	}
	dialSpan.End()
	log.Debug("upstream_dial_ok", "elapsed_ms", dialDur.Milliseconds())

	socks5.TuneSocket(c, cfg.TCPKeepAlive)
	_ = c.SetDeadline(time.Now().Add(cfg.HandshakeTimeout))

	_, hsSpan := lb.tracer.Start(ctx, "upstream.handshake")
	hsStart := time.Now()
	if err := socks5.ClientHandshake(c, u.Username, u.Password); err != nil {
		hsDur := time.Since(hsStart)
		lb.metrics.UpHandshake.WithLabelValues(u.Addr()).Observe(hsDur.Seconds())
		lb.recordFailure(u, "handshake", err.Error())
		log.Warn(
			"upstream_handshake_failed",
			"err",
			err.Error(),
			"elapsed_ms",
			hsDur.Milliseconds(),
		)
		hsSpan.RecordError(err)
		hsSpan.SetStatus(codes.Error, "handshake_failed")
		hsSpan.End()
		_ = c.Close()
		return nil, err
	}
	hsDur := time.Since(hsStart)
	lb.metrics.UpHandshake.WithLabelValues(u.Addr()).Observe(hsDur.Seconds())
	hsSpan.End()

	u.ObserveLatency(dialDur + hsDur)
	log.Debug("upstream_handshake_ok", "elapsed_ms", hsDur.Milliseconds())
	return c, nil
}

// splitHostPort extracts the host part of an "ip:port" or "[ipv6]:port"
// address; returns input unchanged on failure.
func splitHost(addr string) string {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return h
}

// udpBindHost picks the host to bind the client-facing UDP socket on.
// Prefers cfg.UDPBindAddr, then the host portion of cfg.ListenAddr.
func udpBindHost(cfg config.Config) string {
	if cfg.UDPBindAddr != "" {
		return cfg.UDPBindAddr
	}
	return splitHost(cfg.ListenAddr)
}
