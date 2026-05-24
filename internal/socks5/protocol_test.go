package socks5

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"
)

// mockConn is a minimal in-memory net.Conn for protocol tests.
type mockConn struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (m *mockConn) Read(p []byte) (int, error)       { return m.r.Read(p) }
func (m *mockConn) Write(p []byte) (int, error)      { return m.w.Write(p) }
func (m *mockConn) Close() error                     { return nil }
func (m *mockConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(time.Time) error { return nil }

func TestReadGreeting_Accepts_NoAuth(t *testing.T) {
	in := bytes.NewBuffer([]byte{0x05, 0x01, 0x00}) // VER=5, NMETHODS=1, NoAuth
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	if err := ReadGreeting(mc, bufio.NewReader(in)); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []byte{0x05, AuthNone}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("reply=%x want=%x", out.Bytes(), want)
	}
}

func TestReadGreeting_Rejects_Without_NoAuth(t *testing.T) {
	in := bytes.NewBuffer([]byte{0x05, 0x01, 0x02}) // only UserPass offered
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	if err := ReadGreeting(mc, bufio.NewReader(in)); err == nil {
		t.Fatal("expected error on no-NoAuth offer")
	}
	want := []byte{0x05, AuthNoAccept}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("reply=%x want=%x", out.Bytes(), want)
	}
}

func TestReadRequest_Connect_IPv4(t *testing.T) {
	payload := []byte{
		Version, CmdConnect, 0x00, AtypIPv4,
		192, 168, 1, 1,
		0x04, 0x38, // port 1080
	}
	in := bytes.NewBuffer(payload)
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	req, rw, err := ReadRequest(mc, bufio.NewReader(in))
	if err != nil || rw != 0 {
		t.Fatalf("err=%v rw=%d", err, rw)
	}
	if req.Cmd != CmdConnect || req.Atyp != AtypIPv4 ||
		req.Port != 1080 || req.DstLabel != "192.168.1.1" {
		t.Fatalf("bad parse: %+v", req)
	}
}

func TestReadRequest_Bind_Rejected(t *testing.T) {
	payload := []byte{Version, CmdBind, 0x00, AtypIPv4, 0, 0, 0, 0, 0, 0}
	in := bytes.NewBuffer(payload)
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	_, rw, err := ReadRequest(mc, bufio.NewReader(in))
	if err == nil {
		t.Fatal("expected error on BIND")
	}
	if rw != RepCommandNotSupported {
		t.Fatalf("rw=%d want=%d", rw, RepCommandNotSupported)
	}
}

func TestReadRequest_Domain(t *testing.T) {
	host := "example.com"
	payload := []byte{Version, CmdConnect, 0x00, AtypDomain, byte(len(host))}
	payload = append(payload, []byte(host)...)
	payload = append(payload, 0x00, 0x50) // port 80
	in := bytes.NewBuffer(payload)
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	req, _, err := ReadRequest(mc, bufio.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if req.DstLabel != host || req.Port != 80 {
		t.Fatalf("bad: %+v", req)
	}
}

func TestBuildReply_IPv4(t *testing.T) {
	b := BuildReply(RepSuccess, net.ParseIP("127.0.0.1"), 1080)
	want := []byte{Version, RepSuccess, 0x00, AtypIPv4, 127, 0, 0, 1, 0x04, 0x38}
	if !bytes.Equal(b, want) {
		t.Fatalf("got=%x want=%x", b, want)
	}
}

func TestClientHandshake_NoAuth_RoundTrip(t *testing.T) {
	// Simulate server: receive "05 01 00", reply "05 00".
	var serverOut bytes.Buffer
	clientIn := bytes.NewBuffer([]byte{Version, AuthNone})
	mc := &mockConn{r: clientIn, w: &serverOut}

	if err := ClientHandshake(mc, "", ""); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(serverOut.Bytes(), []byte{Version, 0x01, AuthNone}) {
		t.Fatalf("greet=%x", serverOut.Bytes())
	}
}

func TestLabels(t *testing.T) {
	cases := map[byte]string{
		AtypIPv4:   "ipv4",
		AtypIPv6:   "ipv6",
		AtypDomain: "domain",
	}
	for b, want := range cases {
		if got := AtypLabel(b); got != want {
			t.Errorf("AtypLabel(%d)=%s want=%s", b, got, want)
		}
	}
	if CmdLabel(CmdConnect) != "connect" {
		t.Fail()
	}
	if ReplyLabel(RepSuccess) != "success" {
		t.Fail()
	}
}

// Ensure we don't regress on EOF handling.
func TestReadGreeting_EOF(t *testing.T) {
	in := bytes.NewBuffer(nil)
	mc := &mockConn{r: in, w: &bytes.Buffer{}}
	err := ReadGreeting(mc, bufio.NewReader(in))
	// io.ReadFull returns io.ErrUnexpectedEOF on 0-bytes, which may be wrapped.
	// We require a non-nil error and no panic.
	if err == nil {
		t.Fatal("expected an error on empty input, got nil")
	}
	_ = err // suppress unused warning; just verifying no panic and non-nil
}

func TestReadRequest_NonZeroRSV_Rejected(t *testing.T) {
	payload := []byte{Version, CmdConnect, 0x01, AtypIPv4, 192, 168, 1, 1, 0x04, 0x38}
	in := bytes.NewBuffer(payload)
	out := &bytes.Buffer{}
	mc := &mockConn{r: in, w: out}
	_, rw, err := ReadRequest(mc, bufio.NewReader(in))
	if err == nil {
		t.Fatal("expected error on nonzero RSV byte")
	}
	if rw != RepGeneralFailure {
		t.Fatalf("rw=%d want=%d (RepGeneralFailure)", rw, RepGeneralFailure)
	}
}
