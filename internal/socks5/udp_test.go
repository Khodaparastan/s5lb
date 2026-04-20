package socks5

import (
	"bytes"
	"net"
	"testing"
)

func TestUDPDatagram_RoundTrip_IPv4(t *testing.T) {
	payload := []byte("hello, udp")
	encoded, err := EncodeUDPDatagram(net.ParseIP("1.2.3.4"), "", 9999, payload)
	if err != nil {
		t.Fatal(err)
	}
	dg, err := DecodeUDPDatagram(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if dg.Atyp != AtypIPv4 || dg.DstPort != 9999 ||
		dg.DstIP.String() != "1.2.3.4" || !bytes.Equal(dg.Data, payload) {
		t.Fatalf("bad: %+v", dg)
	}
}

func TestUDPDatagram_RoundTrip_Domain(t *testing.T) {
	payload := []byte("x")
	encoded, err := EncodeUDPDatagram(nil, "example.com", 53, payload)
	if err != nil {
		t.Fatal(err)
	}
	dg, err := DecodeUDPDatagram(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if dg.Atyp != AtypDomain || dg.DstHost != "example.com" ||
		dg.DstPort != 53 || !bytes.Equal(dg.Data, payload) {
		t.Fatalf("bad: %+v", dg)
	}
}

func TestUDPDatagram_Rejects_Fragment(t *testing.T) {
	pkt := []byte{0x00, 0x00, 0x01 /*FRAG*/, AtypIPv4, 1, 2, 3, 4, 0, 80}
	if _, err := DecodeUDPDatagram(pkt); err == nil {
		t.Fatal("expected frag rejection")
	}
}

func TestUDPDatagram_Rejects_Truncated(t *testing.T) {
	for _, n := range []int{0, 3, 9} {
		if _, err := DecodeUDPDatagram(make([]byte, n)); err == nil {
			t.Errorf("len=%d: expected error", n)
		}
	}
}

func TestUDPEncode_ErrorOnEmpty(t *testing.T) {
	if _, err := EncodeUDPDatagram(nil, "", 80, []byte("x")); err == nil {
		t.Fatal("expected error on no destination")
	}
}
