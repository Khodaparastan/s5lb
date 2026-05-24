package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	k8sChannelStdin  byte = 0
	k8sChannelStdout byte = 1
	k8sChannelStderr byte = 2
	k8sChannelError  byte = 3

	// rawWSSReadLimit is the maximum frame size we accept from the WebSocket peer.
	// Kubernetes exec stdout frames are typically small; 256 KiB is generous.
	rawWSSReadLimit = 256 * 1024

	// rawWSSCloseDeadline is the maximum time to spend writing a WebSocket close frame.
	rawWSSCloseDeadline = 3 * time.Second
)

// RawWSSExecDialer connects to a direct Kubernetes pod exec WebSocket URL.
//
// This is useful in restricted environments where the caller is given a
// prebuilt wss://.../exec URL and credentials.
type RawWSSExecDialer struct {
	urlTemplate string
	command     []string

	bearerToken     string
	bearerTokenFile string

	caFile                string
	insecureSkipTLSVerify bool
	serverName            string

	headers map[string]string

	stderrLimit int
}

// NewRawWSSExecDialer creates a direct Kubernetes exec WebSocket dialer.
func NewRawWSSExecDialer(cfg KubernetesConfig) (*RawWSSExecDialer, error) {
	if cfg.WSSURL == "" {
		return nil, errors.New("raw WSS exec URL is required")
	}

	parsed, err := url.Parse(cfg.WSSURL)
	if err != nil {
		return nil, fmt.Errorf("parse raw WSS exec URL: %w", err)
	}

	if parsed.Scheme != "wss" && parsed.Scheme != "ws" {
		return nil, fmt.Errorf("raw exec URL must use ws or wss scheme, got %q", parsed.Scheme)
	}

	if parsed.Host == "" {
		return nil, errors.New("raw exec URL host is required")
	}

	return &RawWSSExecDialer{
		urlTemplate:           cfg.WSSURL,
		command:               append([]string(nil), cfg.Command...),
		bearerToken:           cfg.BearerToken,
		bearerTokenFile:       cfg.BearerTokenFile,
		caFile:                cfg.CAFile,
		insecureSkipTLSVerify: cfg.InsecureSkipTLSVerify,
		serverName:            cfg.ServerName,
		headers:               cloneStringMap(cfg.Headers),
		stderrLimit:           32 * 1024,
	}, nil
}

// DialContext returns a net.Conn backed by Kubernetes exec WebSocket channels.
func (d *RawWSSExecDialer) DialContext(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("raw WSS exec dialer only supports tcp, got %q", network)
	}

	host, port, err := splitAddress(address)
	if err != nil {
		return nil, err
	}

	command := expandCommand(d.command, host, port, address)

	execURL, err := d.buildExecURL(host, port, address, command)
	if err != nil {
		return nil, err
	}

	header, err := d.buildHeaders()
	if err != nil {
		return nil, err
	}

	tlsConfig, err := d.tlsConfig()
	if err != nil {
		return nil, err
	}

	dialer := websocket.Dialer{
		TLSClientConfig: tlsConfig,
		Subprotocols: []string{
			"v5.channel.k8s.io",
			"v4.channel.k8s.io",
			"channel.k8s.io",
		},
		EnableCompression: false,
	}

	ws, resp, err := dialer.DialContext(ctx, execURL, header)
	if err != nil {
		status := ""
		if resp != nil {
			status = resp.Status
		}
		if status != "" {
			return nil, fmt.Errorf("dial raw kubernetes exec websocket %s: %w", status, err)
		}

		return nil, fmt.Errorf("dial raw kubernetes exec websocket: %w", err)
	}

	selectedProto := ws.Subprotocol()
	if selectedProto == "" {
		_ = ws.Close()
		return nil, errors.New("kubernetes exec websocket did not negotiate channel protocol")
	}

	local := streamAddr{
		network: "raw-kube-exec-wss",
		address: execURL,
	}
	remote := streamAddr{
		network: network,
		address: address,
	}

	var writeMu sync.Mutex

	// Use context.WithoutCancel so the connection lifetime is independent
	// of the dial context (which may be canceled by the caller after setup).
	connCtx := context.WithoutCancel(ctx)

	conn := newStreamConn(
		connCtx,
		local,
		remote,
		func(b []byte) error {
			frame := make([]byte, 1+len(b))
			frame[0] = k8sChannelStdin
			copy(frame[1:], b)

			writeMu.Lock()
			defer writeMu.Unlock()

			return ws.WriteMessage(websocket.BinaryMessage, frame)
		},
		func() error {
			writeMu.Lock()
			defer writeMu.Unlock()

			// Set a write deadline so a stalled peer cannot block shutdown.
			_ = ws.SetWriteDeadline(time.Now().Add(rawWSSCloseDeadline))
			_ = ws.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			return ws.Close()
		},
	)

	stderr := newLimitedBuffer(d.stderrLimit)

	ws.SetReadLimit(rawWSSReadLimit)
	go rawWSSReadLoop(ws, conn, stderr)

	return conn, nil
}

func (d *RawWSSExecDialer) buildExecURL(
	host, port, address string,
	command []string,
) (string, error) {
	expanded := strings.ReplaceAll(d.urlTemplate, "{{host}}", url.QueryEscape(host))
	expanded = strings.ReplaceAll(expanded, "{{port}}", url.QueryEscape(port))
	expanded = strings.ReplaceAll(expanded, "{{address}}", url.QueryEscape(address))

	u, err := url.Parse(expanded)
	if err != nil {
		return "", fmt.Errorf("parse expanded raw WSS exec URL: %w", err)
	}

	if u.Scheme != "wss" && u.Scheme != "ws" {
		return "", fmt.Errorf("raw exec URL must use ws or wss scheme, got %q", u.Scheme)
	}

	q := u.Query()

	q.Set("stdin", "true")
	q.Set("stdout", "true")
	q.Set("stderr", "true")
	q.Set("tty", "false")

	if len(command) > 0 {
		q.Del("command")
		for _, arg := range command {
			q.Add("command", arg)
		}
	}

	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (d *RawWSSExecDialer) buildHeaders() (http.Header, error) {
	h := http.Header{}

	for k, v := range d.headers {
		h.Set(k, v)
	}

	token := strings.TrimSpace(d.bearerToken)
	if token == "" && d.bearerTokenFile != "" {
		raw, err := os.ReadFile(d.bearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read bearer token file %q: %w", d.bearerTokenFile, err)
		}

		token = strings.TrimSpace(string(raw))
	}

	if token != "" && h.Get("Authorization") == "" {
		h.Set("Authorization", "Bearer "+token)
	}

	return h, nil
}

func (d *RawWSSExecDialer) tlsConfig() (*tls.Config, error) {
	if d.insecureSkipTLSVerify {
		slog.Default().Warn("insecure_skip_tls_verify_enabled",
			"warning", "TLS certificate verification is disabled; do not use in production without additional controls",
		)
	}
	cfg := &tls.Config{
		InsecureSkipVerify: d.insecureSkipTLSVerify, //nolint:gosec
		ServerName:         d.serverName,
		MinVersion:         tls.VersionTLS12,
	}

	if d.caFile == "" {
		return cfg, nil
	}

	raw, err := os.ReadFile(d.caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %q: %w", d.caFile, err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("CA file %q did not contain any PEM certificates", d.caFile)
	}

	cfg.RootCAs = roots
	return cfg, nil
}

func rawWSSReadLoop(ws *websocket.Conn, conn *streamConn, stderr *limitedBuffer) {
	for {
		msgType, payload, err := ws.ReadMessage()
		if err != nil {
			conn.finishRemote(err)
			return
		}

		if msgType != websocket.BinaryMessage && msgType != websocket.TextMessage {
			continue
		}

		if len(payload) == 0 {
			continue
		}

		channel := payload[0]
		data := payload[1:]

		switch channel {
		case k8sChannelStdout:
			if _, err := conn.enqueueRead(data); err != nil {
				conn.finishRemote(err)
				return
			}

		case k8sChannelStderr:
			_, _ = stderr.Write(data)

		case k8sChannelError:
			msg := strings.TrimSpace(string(data))
			stderrText := strings.TrimSpace(stderr.String())

			switch {
			case msg != "" && stderrText != "":
				conn.finishRemote(
					fmt.Errorf("kubernetes exec error: %s: stderr: %s", msg, stderrText),
				)
			case msg != "":
				conn.finishRemote(fmt.Errorf("kubernetes exec error: %s", msg))
			case stderrText != "":
				conn.finishRemote(fmt.Errorf("kubernetes exec stderr: %s", stderrText))
			default:
				conn.finishRemote(errors.New("kubernetes exec error channel closed"))
			}

			return

		default:
			// Ignore resize/unknown channels.
		}
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}

	return out
}
