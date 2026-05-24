package transport

import (
	"fmt"
	"time"

	"github.com/khodaparastan/s5lb/internal/config"
)

// NewFromConfig builds the configured upstream transport dialer.
func NewFromConfig(cfg config.Config) (Dialer, error) {
	switch cfg.Transport.Type {
	case "", config.TransportDirect:
		return TCPDialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}, nil

	case config.TransportKubernetesExec:
		k := cfg.Transport.Kubernetes

		kc := KubernetesConfig{
			Kubeconfig:            k.Kubeconfig,
			Context:               k.Context,
			Namespace:             k.Namespace,
			Pod:                   k.Pod,
			Container:             k.Container,
			Mode:                  k.Mode,
			Command:               k.Command,
			WSSURL:                k.WSSURL,
			BearerToken:           k.BearerToken,
			BearerTokenFile:       k.BearerTokenFile,
			CAFile:                k.CAFile,
			InsecureSkipTLSVerify: k.InsecureSkipTLSVerify,
			ServerName:            k.ServerName,
			Headers:               k.Headers,
		}

		if k.Mode == KubeExecModeRawWSS || k.WSSURL != "" {
			return NewRawWSSExecDialer(kc)
		}

		restCfg, err := BuildRESTConfig(k.Kubeconfig, k.Context)
		if err != nil {
			return nil, err
		}

		return NewKubernetesExecDialer(restCfg, kc)

	default:
		return nil, fmt.Errorf("unsupported transport.type %q", cfg.Transport.Type)
	}
}
