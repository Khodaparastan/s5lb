package socks5

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// UDPDatagram is a decoded SOCKS5 UDP request header (RFC 1928 §7)
// plus the payload.
//
// Wire format:
//
//	+-----+------+------+----------+----------+----------+
//	| RSV | FRAG | ATYP | DST.ADDR | DST.PORT |   DATA   |
//	+-----+------+------+----------+----------+----------+
//	|  2  |   1  |   1  | Variable |    2     | Variable |
//	+-----+------+------+----------+----------+----------+
type UDPDatagram struct {
	Frag    byte
	Atyp    byte
	DstHost string
	DstIP   net.IP // nil for domain atyp
	DstPort uint16
	Data    []byte

	// HeaderLen is the number of bytes consumed by the header (useful when
	// callers want to rewrite/replace only the data portion).
	HeaderLen int
}

// DecodeUDPDatagram parses a SOCKS5 UDP request header + payload from a
// single datagram. Fragmented datagrams (FRAG != 0) are not supported and
// return an error per common practice; RFC technically allows reassembly but
// no real client relies on it.
func DecodeUDPDatagram(pkt []byte) (*UDPDatagram, error) {
	if len(pkt) < 10 {
		return nil, errors.New("udp datagram too short")
	}
	// RSV[0..1] must be 0.
	if pkt[0] != 0x00 || pkt[1] != 0x00 {
		return nil, errors.New("udp rsv nonzero")
	}
	dg := &UDPDatagram{Frag: pkt[2], Atyp: pkt[3]}
	if dg.Frag != 0 {
		return nil, fmt.Errorf("udp fragmentation not supported (frag=%d)", dg.Frag)
	}
	off := 4
	switch dg.Atyp {
	case AtypIPv4:
		if len(pkt) < off+4+2 {
			return nil, errors.New("udp ipv4 truncated")
		}
		dg.DstIP = net.IP(pkt[off : off+4])
		dg.DstHost = dg.DstIP.String()
		off += 4
	case AtypIPv6:
		if len(pkt) < off+16+2 {
			return nil, errors.New("udp ipv6 truncated")
		}
		dg.DstIP = net.IP(pkt[off : off+16])
		dg.DstHost = dg.DstIP.String()
		off += 16
	case AtypDomain:
		if len(pkt) < off+1 {
			return nil, errors.New("udp domain length truncated")
		}
		dl := int(pkt[off])
		off++
		if dl == 0 {
			return nil, errors.New("udp empty domain")
		}
		if len(pkt) < off+dl+2 {
			return nil, errors.New("udp domain truncated")
		}
		dg.DstHost = string(pkt[off : off+dl])
		off += dl
	default:
		return nil, fmt.Errorf("udp bad atyp 0x%02x", dg.Atyp)
	}
	dg.DstPort = binary.BigEndian.Uint16(pkt[off : off+2])
	off += 2
	dg.HeaderLen = off
	dg.Data = pkt[off:]
	return dg, nil
}

// EncodeUDPDatagram builds a SOCKS5 UDP request header in front of `data`.
// Either dstIP OR dstHost must be non-empty. If both are provided, dstIP wins.
func EncodeUDPDatagram(dstIP net.IP, dstHost string, dstPort uint16, data []byte) ([]byte, error) {
	var atyp byte
	var addrBytes []byte
	switch {
	case dstIP != nil:
		if v4 := dstIP.To4(); v4 != nil {
			atyp = AtypIPv4
			addrBytes = v4
		} else {
			atyp = AtypIPv6
			addrBytes = dstIP.To16()
		}
	case dstHost != "":
		if len(dstHost) > 255 {
			return nil, errors.New("udp domain too long")
		}
		atyp = AtypDomain
		addrBytes = append([]byte{byte(len(dstHost))}, []byte(dstHost)...)
	default:
		return nil, errors.New("udp encode: no destination")
	}

	out := make([]byte, 0, 4+len(addrBytes)+2+len(data))
	out = append(out, 0x00, 0x00, 0x00, atyp)
	out = append(out, addrBytes...)
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, dstPort)
	out = append(out, portBuf...)
	out = append(out, data...)
	return out, nil
}
