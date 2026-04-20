package balancer

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/socks5"
)

func newSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// handleClient is the entry point per client TCP connection. It handles the
// SOCKS5 greeting + request parsing, then dispatches to CONNECT or UDP
// handlers based on the request's Cmd byte.
func (lb *LoadBalancer) handleClient(client net.Conn, sess *admission.Session) {
	sid := newSessionID()
	log := lb.log.With(
		"session_id", sid,
		"client", client.RemoteAddr().String(),
	)

	ctx, span := lb.tracer.Start(lb.ctx, "socks5.session")
	span.SetAttributes(
		attribute.String("session.id", sid),
		attribute.String("client.addr", client.RemoteAddr().String()),
	)
	defer span.End()

	lb.track(client)
	defer lb.untrack(client)
	defer client.Close()

	cfg := lb.Config()
	socks5.TuneSocket(client, cfg.TCPKeepAlive)
	br := bufio.NewReaderSize(client, 4096)

	log.Debug("accepted")

	// 1. Greeting.
	_ = client.SetDeadline(time.Now().Add(cfg.HandshakeTimeout))
	greetCtx, greetSpan := lb.tracer.Start(ctx, "socks5.greeting")
	if err := socks5.ReadGreeting(client, br); err != nil {
		lb.metrics.RejectedTotal.WithLabelValues("greet_failed").Inc()
		log.Warn("greet_failed", "err", err.Error())
		greetSpan.RecordError(err)
		greetSpan.SetStatus(codes.Error, "greet_failed")
		greetSpan.End()
		span.SetStatus(codes.Error, "greet_failed")
		return
	}
	greetSpan.End()
	_ = greetCtx

	// 2. Request.
	_, reqSpan := lb.tracer.Start(ctx, "socks5.request")
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
		reqSpan.RecordError(err)
		reqSpan.SetStatus(codes.Error, "request_failed")
		reqSpan.End()
		span.SetStatus(codes.Error, "request_failed")
		return
	}
	reqSpan.SetAttributes(
		attribute.String("socks.cmd", socks5.CmdLabel(req.Cmd)),
		attribute.String("socks.atyp", socks5.AtypLabel(req.Atyp)),
		attribute.String("socks.dst_host", req.DstLabel),
		attribute.Int("socks.dst_port", int(req.Port)),
	)
	reqSpan.End()

	log = log.With(
		"cmd", socks5.CmdLabel(req.Cmd),
		"dst", req.DstLabel,
		"dst_port", req.Port,
		"atyp", socks5.AtypLabel(req.Atyp),
	)
	lb.metrics.SocksRequest.WithLabelValues(socks5.AtypLabel(req.Atyp), "ok").Inc()

	// Clear handshake deadline before potentially long queue wait.
	_ = client.SetDeadline(time.Time{})

	switch req.Cmd {
	case socks5.CmdConnect:
		lb.handleConnect(ctx, client, br, req, sess, log)
	case socks5.CmdUDPAssociate:
		if !cfg.UDPEnabled {
			_, _ = client.Write(socks5.ReplyBytes(socks5.RepCommandNotSupported))
			lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepCommandNotSupported)).Inc()
			log.Warn("udp_disabled")
			return
		}
		lb.handleUDPAssociate(ctx, client, br, req, sess, log)
	default:
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepCommandNotSupported))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepCommandNotSupported)).Inc()
	}
}

// logWithUpstream decorates the session logger with upstream info.
func logWithUpstream(log *slog.Logger, addr string) *slog.Logger {
	return log.With("upstream", addr)
}
