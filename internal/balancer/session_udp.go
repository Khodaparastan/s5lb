package balancer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/strategy"
)

// pinnedClient holds the (IP, port) of the client we first saw a UDP datagram
// from on this association. All downstream datagrams are sent back there.
type pinnedClient struct {
	mu   sync.RWMutex
	addr *net.UDPAddr
}

func (p *pinnedClient) set(a *net.UDPAddr) {
	p.mu.Lock()
	p.addr = a
	p.mu.Unlock()
}
func (p *pinnedClient) get() *net.UDPAddr {
	p.mu.RLock()
	a := p.addr
	p.mu.RUnlock()
	return a
}

// udpStats is shared between the two relay goroutines. Atomic because both
// goroutines run concurrently.
type udpStats struct {
	pktsUp, pktsDown   atomic.Int64
	bytesUp, bytesDown atomic.Int64
}

// handleUDPAssociate implements CMD=UDP_ASSOCIATE (RFC 1928 §6-7).
//
// Lifecycle:
//   - Pick upstream, dial + SOCKS5 handshake over TCP (the control conn).
//   - Issue UDP_ASSOCIATE on the control conn; upstream returns its UDP
//     relay endpoint in BND.ADDR/BND.PORT.
//   - Bind a local UDP socket facing the client and one facing the upstream.
//   - Reply to the client with our local UDP endpoint.
//   - Relay datagrams bidirectionally with SOCKS5 UDP headers verbatim.
//   - Any of: control TCP close, client TCP close, ctx cancel, idle timeout
//     on either UDP socket -> tear everything down.
func (lb *LoadBalancer) handleUDPAssociate(
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

	// TCP control conn — stays open for the life of the association.
	upTCP, err := lb.dialAndHandshakeUpstream(ctx, up, log)
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		return
	}
	lb.track(upTCP)
	defer lb.untrack(upTCP)
	defer upTCP.Close()

	// UDP_ASSOCIATE on control conn.
	_, uaSpan := lb.tracer.Start(ctx, "upstream.udp_associate")
	uaStart := time.Now()
	reply, err := socks5.ClientUDPAssociate(upTCP, req.Atyp, req.RawAddr, req.Port)
	if err != nil {
		lb.recordFailure(up, "udp_associate", err.Error())
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("upstream_udp_associate_failed",
			"err", err.Error(),
			"elapsed_ms", time.Since(uaStart).Milliseconds())
		uaSpan.RecordError(err)
		uaSpan.SetStatus(codes.Error, "udp_associate_failed")
		uaSpan.End()
		return
	}
	uaSpan.SetAttributes(attribute.String("socks.reply", socks5.ReplyLabel(reply.Rep)))
	uaSpan.End()
	if reply.Rep != socks5.RepSuccess {
		_, _ = client.Write(socks5.ReplyBytes(reply.Rep))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(reply.Rep)).Inc()
		log.Warn("upstream_udp_associate_rejected", "reply", socks5.ReplyLabel(reply.Rep))
		return
	}
	lb.recordSuccess(up)

	// Resolve the upstream UDP endpoint; substitute 0.0.0.0/:: with TCP host.
	upUDPAddr, err := resolveUpstreamUDPAddr(reply, up.Host)
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("upstream_udp_addr_resolve_failed", "err", err.Error())
		return
	}

	// Bind local UDP sockets.
	bindHost := udpBindHost(cfg)
	bindIP := net.ParseIP(bindHost)
	if bindIP == nil {
		bindIP = net.IPv4zero
	}
	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: bindIP, Port: 0})
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("udp_client_bind_failed", "err", err.Error(), "bind_host", bindHost)
		return
	}
	defer clientPC.Close()

	upPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		_, _ = client.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
		lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepGeneralFailure)).Inc()
		log.Warn("udp_upstream_bind_failed", "err", err.Error())
		return
	}
	defer upPC.Close()

	// Reply to client with our local UDP endpoint.
	localAddr := clientPC.LocalAddr().(*net.UDPAddr)
	replyBytes := socks5.BuildReply(socks5.RepSuccess, localAddr.IP, uint16(localAddr.Port))
	if _, err := client.Write(replyBytes); err != nil {
		log.Warn("udp_reply_write_failed", "err", err.Error())
		return
	}
	lb.metrics.SocksReply.WithLabelValues(socks5.ReplyLabel(socks5.RepSuccess)).Inc()
	lb.metrics.UDPAssocActive.Inc()
	defer lb.metrics.UDPAssocActive.Dec()

	log.Info("udp_session_start",
		"local_udp", localAddr.String(),
		"upstream_udp", upUDPAddr.String(),
	)
	sessStart := time.Now()

	// Per-session shared state.
	pin := &pinnedClient{}
	stats := &udpStats{}

	relayCtx, relayCancel := context.WithCancel(ctx)
	defer relayCancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// client -> upstream
	go func() {
		defer wg.Done()
		defer relayCancel()
		lb.udpClientToUpstream(relayCtx, clientPC, upPC, upUDPAddr, pin, stats, log)
	}()

	// upstream -> client
	go func() {
		defer wg.Done()
		defer relayCancel()
		lb.udpUpstreamToClient(relayCtx, upPC, clientPC, pin, stats, log)
	}()

	// TCP control conn watchdog: any read unblocks iff the control conn is
	// closing. Per RFC the client SHOULD NOT send data on the control conn,
	// so we treat *any* data or error as "session over".
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer relayCancel()
		buf := make([]byte, 1)
		for {
			select {
			case <-relayCtx.Done():
				return
			default:
			}
			_ = client.SetReadDeadline(time.Now().Add(cfg.UDPIdleTimeout))
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()

	// Wait for any path to end the session.
	<-relayCtx.Done()
	_ = clientPC.SetDeadline(time.Now())
	_ = upPC.SetDeadline(time.Now())
	_ = client.SetReadDeadline(time.Now())
	wg.Wait()

	lb.metrics.UDPBytes.WithLabelValues("upstream").Add(float64(stats.bytesUp.Load()))
	lb.metrics.UDPBytes.WithLabelValues("downstream").Add(float64(stats.bytesDown.Load()))
	lb.metrics.UDPPackets.WithLabelValues("upstream").Add(float64(stats.pktsUp.Load()))
	lb.metrics.UDPPackets.WithLabelValues("downstream").Add(float64(stats.pktsDown.Load()))
	lb.metrics.UDPSessionDuration.Observe(time.Since(sessStart).Seconds())

	log.Info("udp_session_end",
		"duration_ms", time.Since(sessStart).Milliseconds(),
		"pkts_up", stats.pktsUp.Load(),
		"pkts_down", stats.pktsDown.Load(),
		"bytes_up", stats.bytesUp.Load(),
		"bytes_down", stats.bytesDown.Load(),
	)
}

// udpClientToUpstream reads from the client-facing UDP socket, validates the
// SOCKS5 UDP header, pins the source on first packet, and forwards verbatim
// onto the upstream relay endpoint.
func (lb *LoadBalancer) udpClientToUpstream(
	ctx context.Context,
	clientPC, upPC *net.UDPConn,
	upUDPAddr *net.UDPAddr,
	pin *pinnedClient,
	stats *udpStats,
	log *slog.Logger,
) {
	cfg := lb.Config()
	buf := make([]byte, 64*1024)

	for {
		if ctx.Err() != nil {
			return
		}
		_ = clientPC.SetReadDeadline(time.Now().Add(cfg.UDPIdleTimeout))
		n, src, err := clientPC.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				log.Debug("udp_client_idle_timeout")
				return
			}
			log.Warn("udp_client_read_err", "err", err.Error())
			return
		}

		// Enforce source pinning.
		if cur := pin.get(); cur == nil {
			pin.set(src)
			log.Debug("udp_client_pinned", "addr", src.String())
		} else if !udpAddrEqual(cur, src) {
			lb.metrics.UDPDropped.WithLabelValues("foreign_src").Inc()
			continue
		}

		dg, err := socks5.DecodeUDPDatagram(buf[:n])
		if err != nil {
			lb.metrics.UDPDropped.WithLabelValues("decode_err_client").Inc()
			log.Debug("udp_decode_err_client", "err", err.Error())
			continue
		}

		// Forward raw (upstream re-parses the same SOCKS5 UDP header).
		if _, err := upPC.WriteToUDP(buf[:n], upUDPAddr); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			lb.metrics.UDPDropped.WithLabelValues("upstream_write_err").Inc()
			log.Warn("udp_upstream_write_err", "err", err.Error())
			continue
		}
		stats.pktsUp.Add(1)
		stats.bytesUp.Add(int64(len(dg.Data)))
	}
}

// udpUpstreamToClient reads from the upstream UDP relay and forwards verbatim
// to the pinned client address. Datagrams received before the client has sent
// anything are dropped (no-pin).
func (lb *LoadBalancer) udpUpstreamToClient(
	ctx context.Context,
	upPC, clientPC *net.UDPConn,
	pin *pinnedClient,
	stats *udpStats,
	log *slog.Logger,
) {
	cfg := lb.Config()
	buf := make([]byte, 64*1024)

	for {
		if ctx.Err() != nil {
			return
		}
		_ = upPC.SetReadDeadline(time.Now().Add(cfg.UDPIdleTimeout))
		n, _, err := upPC.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				log.Debug("udp_upstream_idle_timeout")
				return
			}
			log.Warn("udp_upstream_read_err", "err", err.Error())
			return
		}

		dg, err := socks5.DecodeUDPDatagram(buf[:n])
		if err != nil {
			lb.metrics.UDPDropped.WithLabelValues("decode_err_upstream").Inc()
			log.Debug("udp_upstream_decode_err", "err", err.Error())
			continue
		}

		clientAddr := pin.get()
		if clientAddr == nil {
			lb.metrics.UDPDropped.WithLabelValues("no_pin").Inc()
			continue
		}
		if _, err := clientPC.WriteToUDP(buf[:n], clientAddr); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			lb.metrics.UDPDropped.WithLabelValues("client_write_err").Inc()
			log.Warn("udp_client_write_err", "err", err.Error())
			continue
		}
		stats.pktsDown.Add(1)
		stats.bytesDown.Add(int64(len(dg.Data)))
	}
}

// resolveUpstreamUDPAddr interprets the upstream's BND.* reply for
// UDP_ASSOCIATE. Substitutes unspecified addresses (0.0.0.0/::) with the
// upstream's TCP host, per common SOCKS5 server behavior.
func resolveUpstreamUDPAddr(reply socks5.ClientRequestReply, tcpHost string) (*net.UDPAddr, error) {
	var host string
	switch {
	case reply.BindIP != nil:
		if reply.BindIP.IsUnspecified() {
			host = tcpHost
		} else {
			host = reply.BindIP.String()
		}
	case reply.BindHost != "":
		host = reply.BindHost
	default:
		return nil, errors.New("empty bind address in UDP_ASSOCIATE reply")
	}
	return net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(int(reply.BindPort))))
}

// udpAddrEqual compares IP and port; zone is intentionally ignored.
func udpAddrEqual(a, b *net.UDPAddr) bool {
	return a.Port == b.Port && a.IP.Equal(b.IP)
}
