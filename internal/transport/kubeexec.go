package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// KubernetesExecDialer opens a net.Conn through Kubernetes pod exec.
//
// It execs a command inside a pod/container. The command must connect its
// stdin/stdout to the requested host:port.
type KubernetesExecDialer struct {
	restConfig *rest.Config
	client     kubernetes.Interface

	namespace string
	pod       string
	container string

	mode    string
	command []string

	stderrLimit int
}

// NewKubernetesExecDialer creates a Kubernetes exec transport using client-go.
func NewKubernetesExecDialer(
	restConfig *rest.Config,
	cfg KubernetesConfig,
) (*KubernetesExecDialer, error) {
	if restConfig == nil {
		return nil, errors.New("kubernetes rest config is nil")
	}

	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}

	if cfg.Pod == "" {
		return nil, errors.New("kubernetes exec pod is required")
	}

	if len(cfg.Command) == 0 {
		return nil, errors.New("kubernetes exec command is required")
	}

	if cfg.Mode == "" {
		cfg.Mode = KubeExecModeWebSocketPreferred
	}

	switch cfg.Mode {
	case KubeExecModeSPDY, KubeExecModeWebSocket, KubeExecModeWebSocketPreferred:
	default:
		return nil, fmt.Errorf("unsupported client-go kubernetes exec mode %q", cfg.Mode)
	}

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return &KubernetesExecDialer{
		restConfig:  rest.CopyConfig(restConfig),
		client:      client,
		namespace:   cfg.Namespace,
		pod:         cfg.Pod,
		container:   cfg.Container,
		mode:        cfg.Mode,
		command:     append([]string(nil), cfg.Command...),
		stderrLimit: 32 * 1024,
	}, nil
}

// DialContext returns a net.Conn backed by a Kubernetes exec stream.
func (d *KubernetesExecDialer) DialContext(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("kubernetes exec dialer only supports tcp, got %q", network)
	}

	host, port, err := splitAddress(address)
	if err != nil {
		return nil, err
	}

	command := expandCommand(d.command, host, port, address)
	execURL := d.execURL(command)

	stdinR, stdinW := io.Pipe()

	local := streamAddr{
		network: "kube-exec",
		address: d.namespace + "/" + d.pod,
	}
	remote := streamAddr{
		network: network,
		address: address,
	}

	stderr := newLimitedBuffer(d.stderrLimit)

	executor, err := d.executor(execURL)
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		return nil, err
	}

	// Check context before committing to connection lifetime.
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		return nil, ctxErr
	}

	// Use context.WithoutCancel so the connection lifetime is independent
	// of the dial context (which may be canceled by the caller after setup).
	connParent := context.WithoutCancel(ctx)

	conn := newStreamConn(
		connParent,
		local,
		remote,
		func(b []byte) error {
			_, err := stdinW.Write(b)
			return err
		},
		func() error {
			_ = stdinR.Close()
			return stdinW.Close()
		},
	)

	go func() {
		// Use conn.ctx (not connParent): streamConn.Close cancels conn.ctx so
		// the Kubernetes exec stream is stopped promptly on connection close.
		streamErr := executor.StreamWithContext(conn.ctx, remotecommand.StreamOptions{
			Stdin:  stdinR,
			Stdout: remoteStdoutWriter{conn: conn},
			Stderr: stderr,
			Tty:    false,
		})

		if streamErr != nil {
			errText := strings.TrimSpace(stderr.String())
			if errText != "" {
				streamErr = fmt.Errorf("%w: remote stderr: %s", streamErr, errText)
			}
		}

		conn.finishRemote(streamErr)
	}()

	return conn, nil
}

func splitAddress(address string) (string, string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", "", fmt.Errorf("split upstream address %q: %w", address, err)
	}

	if host == "" || port == "" {
		return "", "", fmt.Errorf("invalid upstream address %q", address)
	}

	if _, err := strconv.Atoi(port); err != nil {
		return "", "", fmt.Errorf("invalid upstream port %q: %w", port, err)
	}

	return host, port, nil
}

func expandCommand(template []string, host, port, address string) []string {
	out := make([]string, 0, len(template))

	for _, part := range template {
		part = strings.ReplaceAll(part, "{{host}}", host)
		part = strings.ReplaceAll(part, "{{port}}", port)
		part = strings.ReplaceAll(part, "{{address}}", address)
		out = append(out, part)
	}

	return out
}

func (d *KubernetesExecDialer) execURL(command []string) *url.URL {
	req := d.client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Namespace(d.namespace).
		Name(d.pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: d.container,
			Command:   command,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	return req.URL()
}

func (d *KubernetesExecDialer) executor(execURL *url.URL) (remotecommand.Executor, error) {
	switch d.mode {
	case KubeExecModeSPDY:
		return remotecommand.NewSPDYExecutor(d.restConfig, http.MethodPost, execURL)

	case KubeExecModeWebSocket:
		return newClientGoWebSocketExecutor(d.restConfig, execURL)

	case KubeExecModeWebSocketPreferred:
		wsExec, wsErr := newClientGoWebSocketExecutor(d.restConfig, execURL)
		if wsErr == nil {
			return wsExec, nil
		}

		spdyExec, spdyErr := remotecommand.NewSPDYExecutor(d.restConfig, http.MethodPost, execURL)
		if spdyErr != nil {
			return nil, fmt.Errorf("websocket executor: %w", errors.Join(wsErr, spdyErr))
		}

		return spdyExec, nil

	default:
		return nil, fmt.Errorf("unsupported kubernetes exec mode %q", d.mode)
	}
}

// newClientGoWebSocketExecutor uses client-go's WebSocket executor.
//
// If your pinned client-go version exposes a different signature, adjust only
// this function.
func newClientGoWebSocketExecutor(
	restConfig *rest.Config,
	execURL *url.URL,
) (remotecommand.Executor, error) {
	return remotecommand.NewWebSocketExecutor(restConfig, http.MethodGet, execURL.String())
}
