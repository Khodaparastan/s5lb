package socks5

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	Version = 0x05

	AuthNone     = 0x00
	AuthUserPass = 0x02
	AuthNoAccept = 0xFF

	CmdConnect = 0x01

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

// ReplyBytes returns a minimal BND.ADDR=0.0.0.0 BND.PORT=0 reply payload.
func ReplyBytes(rep byte) []byte {
	return []byte{Version, rep, 0x00, AtypIPv4, 0, 0, 0, 0, 0, 0}
}

// ReadGreeting handles the SOCKS5 method-negotiation greeting and replies
// with AuthNone. Returns an error on protocol violations.
func ReadGreeting(client net.Conn, br *bufio.Reader) error {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return err
	}
	if hdr[0] != Version || hdr[1] == 0 {
		return errors.New("bad greeting")
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(br, methods); err != nil {
		return err
	}
	_, err := client.Write([]byte{Version, AuthNone})
	return err
}

// Request is a parsed client CONNECT request.
type Request struct {
	Atyp     byte
	RawAddr  []byte // For domain: includes length prefix (suitable for relay).
	Port     uint16
	DstLabel string // Human-readable "host" (no port).
}

// ReadRequest parses a SOCKS5 CONNECT request from the client.
// On unsupported CMD/ATYP it writes the appropriate reply and returns an error.
func ReadRequest(client net.Conn, br *bufio.Reader) (*Request, byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return nil, 0, err
	}
	if hdr[0] != Version {
		return nil, 0, errors.New("bad request version")
	}
	if hdr[1] != CmdConnect {
		_, _ = client.Write(ReplyBytes(RepCommandNotSupported))
		return nil, RepCommandNotSupported, errors.New("unsupported cmd")
	}
	atyp := hdr[3]
	raw, port, label, err := readDest(br, atyp)
	if err != nil {
		_, _ = client.Write(ReplyBytes(RepAddrTypeNotSupported))
		return nil, RepAddrTypeNotSupported, err
	}
	return &Request{Atyp: atyp, RawAddr: raw, Port: port, DstLabel: label}, 0, nil
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
		return nil, 0, "", errors.New("unsupported atyp")
	}
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, portBuf); err != nil {
		return nil, 0, "", err
	}
	return raw, binary.BigEndian.Uint16(portBuf), label, nil
}

// ClientHandshake performs the method negotiation + optional user/pass
// authentication as a SOCKS5 *client* (used when dialing an upstream).
func ClientHandshake(conn net.Conn, user, pass string) error {
	haveCreds := user != "" || pass != ""
	var greet []byte
	if haveCreds {
		greet = []byte{Version, 0x02, AuthNone, AuthUserPass}
	} else {
		greet = []byte{Version, 0x01, AuthNone}
	}
	if _, err := conn.Write(greet); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != Version {
		return fmt.Errorf("bad version %d", resp[0])
	}
	switch resp[1] {
	case AuthNone:
		return nil
	case AuthUserPass:
		if !haveCreds {
			return errors.New("upstream requires auth")
		}
		u, p := []byte(user), []byte(pass)
		if len(u) > 255 || len(p) > 255 {
			return errors.New("credential too long")
		}
		req := make([]byte, 0, 3+len(u)+len(p))
		req = append(req, 0x01, byte(len(u)))
		req = append(req, u...)
		req = append(req, byte(len(p)))
		req = append(req, p...)
		if _, err := conn.Write(req); err != nil {
			return err
		}
		ar := make([]byte, 2)
		if _, err := io.ReadFull(conn, ar); err != nil {
			return err
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

// ClientConnect issues CONNECT over an authenticated session and returns the
// reply code from the upstream.
func ClientConnect(conn net.Conn, atyp byte, rawAddr []byte, port uint16) (byte, error) {
	req := make([]byte, 0, 6+len(rawAddr))
	req = append(req, Version, CmdConnect, 0x00, atyp)
	req = append(req, rawAddr...)
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, port)
	req = append(req, portBuf...)
	if _, err := conn.Write(req); err != nil {
		return 0, err
	}

	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return 0, err
	}
	if hdr[0] != Version {
		return 0, errors.New("bad reply version")
	}
	rep := hdr[1]

	switch hdr[3] {
	case AtypIPv4:
		if _, err := io.ReadFull(conn, make([]byte, 4)); err != nil {
			return 0, err
		}
	case AtypIPv6:
		if _, err := io.ReadFull(conn, make([]byte, 16)); err != nil {
			return 0, err
		}
	case AtypDomain:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(conn, lb); err != nil {
			return 0, err
		}
		if _, err := io.ReadFull(conn, make([]byte, lb[0])); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("bad reply atyp 0x%02x", hdr[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		return 0, err
	}
	return rep, nil
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
