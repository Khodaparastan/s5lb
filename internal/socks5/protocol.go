// Package socks5 implements the RFC 1928 / RFC 1929 wire protocol primitives
// used by both the frontend (serving clients) and the backend (dialing
// upstreams), plus the SOCKS5 UDP request header codec.
package socks5

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Protocol constants.
const (
	Version = 0x05

	AuthNone     = 0x00
	AuthUserPass = 0x02
	AuthNoAccept = 0xFF

	CmdConnect      = 0x01
	CmdBind         = 0x02
	CmdUDPAssociate = 0x03

	AtypIPv4   = 0x01
	AtypDomain = 0x03
	AtypIPv6   = 0x04

	RepSuccess              = 0x00
	RepGeneralFailure       = 0x01
	RepConnNotAllowed       = 0x02
	RepNetworkUnreachable   = 0x03
	RepHostUnreachable      = 0x04
	RepConnRefused          = 0x05
	RepTTLExpired           = 0x06
	RepCommandNotSupported  = 0x07
	RepAddrTypeNotSupported = 0x08
)

// ReplyBytes returns a minimal reply payload with BND.ADDR = 0.0.0.0, BND.PORT = 0.
func ReplyBytes(rep byte) []byte {
	return []byte{Version, rep, 0x00, AtypIPv4, 0, 0, 0, 0, 0, 0}
}

// BuildReply constructs a SOCKS5 reply with the given bind address/port.
// If bindHost/bindPort are zero, emits the minimal 0.0.0.0:0 form.
func BuildReply(rep byte, bindIP net.IP, bindPort uint16) []byte {
	if bindIP == nil {
		return ReplyBytes(rep)
	}

	var atyp byte
	var addr []byte

	if v4 := bindIP.To4(); v4 != nil {
		atyp = AtypIPv4
		addr = v4
	} else if v6 := bindIP.To16(); v6 != nil {
		atyp = AtypIPv6
		addr = v6
	} else {
		return ReplyBytes(rep)
	}

	out := make([]byte, 0, 4+len(addr)+2)
	out = append(out, Version, rep, 0x00, atyp)
	out = append(out, addr...)

	var portBuf [2]byte
	binary.BigEndian.PutUint16(portBuf[:], bindPort)
	out = append(out, portBuf[:]...)

	return out
}

// ReadGreeting performs RFC 1928 method negotiation on the client side.
// We only support NoAuth on the frontend in this release; any client that
// doesn't offer NoAuth is rejected per RFC with reply 0xFF.
func ReadGreeting(client net.Conn, br *bufio.Reader) error {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return fmt.Errorf("read greeting header: %w", err)
	}
	if hdr[0] != Version || hdr[1] == 0 {
		return errors.New("bad greeting")
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(br, methods); err != nil {
		return fmt.Errorf("read methods: %w", err)
	}

	accepted := false
	for _, m := range methods {
		if m == AuthNone {
			accepted = true
			break
		}
	}
	if !accepted {
		_, _ = client.Write([]byte{Version, AuthNoAccept})
		return errors.New("no acceptable auth method (client did not offer NoAuth)")
	}
	if _, err := client.Write([]byte{Version, AuthNone}); err != nil {
		return fmt.Errorf("write greeting reply: %w", err)
	}
	return nil
}

// Request is a parsed SOCKS5 client request.
type Request struct {
	Cmd      byte
	Atyp     byte
	RawAddr  []byte // Domain includes the 1-byte length prefix.
	Port     uint16
	DstLabel string // Human-readable host (no port).
}

// ReadRequest parses a SOCKS5 request header. Supports CONNECT and
// UDP_ASSOCIATE; BIND is rejected with RepCommandNotSupported. On protocol
// errors we write the appropriate reply to `client` before returning.
//
// Returns (req, replyWrittenCode, err). `replyWrittenCode` is nonzero iff
// this function wrote a reply to the client.
func ReadRequest(client net.Conn, br *bufio.Reader) (*Request, byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return nil, 0, fmt.Errorf("read request header: %w", err)
	}
	if hdr[0] != Version {
		return nil, 0, errors.New("bad request version")
	}
	if hdr[2] != 0x00 {
		_, _ = client.Write(ReplyBytes(RepGeneralFailure))
		return nil, RepGeneralFailure, fmt.Errorf("reserved byte must be 0x00, got 0x%02x", hdr[2])
	}
	cmd := hdr[1]
	if cmd != CmdConnect && cmd != CmdUDPAssociate {
		_, _ = client.Write(ReplyBytes(RepCommandNotSupported))
		return nil, RepCommandNotSupported, fmt.Errorf("unsupported cmd 0x%02x", cmd)
	}
	atyp := hdr[3]
	raw, port, label, err := readDest(br, atyp)
	if err != nil {
		_, _ = client.Write(ReplyBytes(RepAddrTypeNotSupported))
		return nil, RepAddrTypeNotSupported, err
	}
	return &Request{Cmd: cmd, Atyp: atyp, RawAddr: raw, Port: port, DstLabel: label}, 0, nil
}

func readDest(r io.Reader, atyp byte) ([]byte, uint16, string, error) {
	var raw []byte
	var label string
	switch atyp {
	case AtypIPv4:
		raw = make([]byte, 4)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, 0, "", err
		}
		label = net.IP(raw).String()
	case AtypIPv6:
		raw = make([]byte, 16)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, 0, "", err
		}
		label = net.IP(raw).String()
	case AtypDomain:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(r, lb); err != nil {
			return nil, 0, "", err
		}
		if lb[0] == 0 {
			return nil, 0, "", errors.New("empty domain")
		}
		dom := make([]byte, lb[0])
		if _, err := io.ReadFull(r, dom); err != nil {
			return nil, 0, "", err
		}
		label = string(dom)
		raw = append(lb, dom...)
	default:
		return nil, 0, "", fmt.Errorf("unsupported atyp 0x%02x", atyp)
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(r, portBuf[:]); err != nil {
		return nil, 0, "", err
	}
	return raw, binary.BigEndian.Uint16(portBuf[:]), label, nil
}

// ClientHandshake performs RFC 1928 method negotiation + optional RFC 1929
// user/pass authentication as a SOCKS5 *client* (when dialing an upstream).
func ClientHandshake(conn net.Conn, user, pass string) error {
	haveCreds := user != "" || pass != ""

	var greet []byte
	if haveCreds {
		greet = []byte{Version, 0x02, AuthNone, AuthUserPass}
	} else {
		greet = []byte{Version, 0x01, AuthNone}
	}

	if _, err := conn.Write(greet); err != nil {
		return fmt.Errorf("write greeting: %w", err)
	}

	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("read greeting reply: %w", err)
	}

	if resp[0] != Version {
		return fmt.Errorf("bad version %d", resp[0])
	}

	switch resp[1] {
	case AuthNone:
		return nil

	case AuthUserPass:
		if !haveCreds {
			return errors.New("upstream requires auth but no credentials configured")
		}

		u, p := []byte(user), []byte(pass)
		if len(u) > 255 || len(p) > 255 {
			return errors.New("credential exceeds 255 bytes")
		}

		req := make([]byte, 0, 3+len(u)+len(p))
		req = append(req, 0x01, byte(len(u)))
		req = append(req, u...)
		req = append(req, byte(len(p)))
		req = append(req, p...)

		if _, err := conn.Write(req); err != nil {
			return fmt.Errorf("write auth: %w", err)
		}

		ar := make([]byte, 2)
		if _, err := io.ReadFull(conn, ar); err != nil {
			return fmt.Errorf("read auth reply: %w", err)
		}

		if ar[0] != 0x01 {
			return fmt.Errorf("bad auth reply version %d", ar[0])
		}

		if ar[1] != 0x00 {
			return errors.New("auth rejected")
		}

		return nil

	case AuthNoAccept:
		return errors.New("no acceptable auth methods")

	default:
		return fmt.Errorf("unsupported auth method 0x%02x", resp[1])
	}
}

// ClientRequestReply is the reply summary of a SOCKS5 request from an upstream.
type ClientRequestReply struct {
	Rep      byte
	BindAtyp byte
	BindIP   net.IP // populated for IPv4/IPv6 replies
	BindHost string // populated for Domain replies
	BindPort uint16
}

// ClientConnect issues CONNECT over an authenticated session and returns
// the upstream's reply (including the bind address, which matters for UDP).
func ClientConnect(
	conn net.Conn,
	atyp byte,
	rawAddr []byte,
	port uint16,
) (ClientRequestReply, error) {
	return clientRequest(conn, CmdConnect, atyp, rawAddr, port)
}

// ClientUDPAssociate issues UDP_ASSOCIATE and returns the upstream's reply.
// The bind address in the reply is the upstream's UDP relay endpoint.
func ClientUDPAssociate(
	conn net.Conn,
	atyp byte,
	rawAddr []byte,
	port uint16,
) (ClientRequestReply, error) {
	return clientRequest(conn, CmdUDPAssociate, atyp, rawAddr, port)
}

func clientRequest(
	conn net.Conn,
	cmd, atyp byte,
	rawAddr []byte,
	port uint16,
) (ClientRequestReply, error) {
	var out ClientRequestReply

	var portBufReq [2]byte
	binary.BigEndian.PutUint16(portBufReq[:], port)
	req := make([]byte, 0, 6+len(rawAddr))
	req = append(req, Version, cmd, 0x00, atyp)
	req = append(req, rawAddr...)
	req = append(req, portBufReq[:]...)
	if _, err := conn.Write(req); err != nil {
		return out, fmt.Errorf("write request: %w", err)
	}

	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return out, fmt.Errorf("read reply header: %w", err)
	}
	if hdr[0] != Version {
		return out, errors.New("bad reply version")
	}
	if hdr[2] != 0x00 {
		return out, fmt.Errorf("reply reserved byte must be 0x00, got 0x%02x", hdr[2])
	}
	out.Rep = hdr[1]
	out.BindAtyp = hdr[3]

	switch hdr[3] {
	case AtypIPv4:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return out, err
		}
		out.BindIP = net.IP(buf)
	case AtypIPv6:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return out, err
		}
		out.BindIP = net.IP(buf)
	case AtypDomain:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(conn, lb); err != nil {
			return out, err
		}
		dom := make([]byte, lb[0])
		if _, err := io.ReadFull(conn, dom); err != nil {
			return out, err
		}
		out.BindHost = string(dom)
	default:
		return out, fmt.Errorf("bad reply atyp 0x%02x", hdr[3])
	}
	var pb [2]byte
	if _, err := io.ReadFull(conn, pb[:]); err != nil {
		return out, err
	}
	out.BindPort = binary.BigEndian.Uint16(pb[:])
	return out, nil
}

// AtypLabel returns a metric-safe string for an address type.
func AtypLabel(a byte) string {
	switch a {
	case AtypIPv4:
		return "ipv4"
	case AtypIPv6:
		return "ipv6"
	case AtypDomain:
		return "domain"
	default:
		return "unknown"
	}
}

// CmdLabel returns a metric-safe string for a command byte.
func CmdLabel(c byte) string {
	switch c {
	case CmdConnect:
		return "connect"
	case CmdBind:
		return "bind"
	case CmdUDPAssociate:
		return "udp_associate"
	default:
		return fmt.Sprintf("0x%02x", c)
	}
}

// ReplyLabel returns a metric-safe string for a reply code.
func ReplyLabel(r byte) string {
	switch r {
	case RepSuccess:
		return "success"
	case RepGeneralFailure:
		return "general_failure"
	case RepConnNotAllowed:
		return "not_allowed"
	case RepNetworkUnreachable:
		return "net_unreachable"
	case RepHostUnreachable:
		return "host_unreachable"
	case RepConnRefused:
		return "refused"
	case RepTTLExpired:
		return "ttl_expired"
	case RepCommandNotSupported:
		return "cmd_unsupported"
	case RepAddrTypeNotSupported:
		return "atyp_unsupported"
	default:
		return fmt.Sprintf("0x%02x", r)
	}
}
