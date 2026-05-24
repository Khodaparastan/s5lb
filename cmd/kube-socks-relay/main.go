package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "kube-socks-relay: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 4 {
		return fmt.Errorf("usage: kube-socks-relay tcp host port")
	}

	network := os.Args[1]
	host := os.Args[2]
	port := os.Args[3]

	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return fmt.Errorf("unsupported network %q", network)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	address := net.JoinHostPort(host, port)

	dialer := net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	defer conn.Close()

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(conn, os.Stdin)

		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}

		errCh <- normalizeCopyErr(err)
	}()

	go func() {
		_, err := io.Copy(os.Stdout, conn)
		errCh <- normalizeCopyErr(err)
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil

	case err := <-errCh:
		_ = conn.Close()
		if err != nil {
			return err
		}

		return nil
	}
}

func normalizeCopyErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	if errors.Is(err, net.ErrClosed) {
		return nil
	}

	return err
}
