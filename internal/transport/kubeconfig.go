package transport

import (
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// KubernetesConfig identifies the Kubernetes API target and exec destination.
type KubernetesConfig struct {
	// Kubeconfig is optional. Empty means in-cluster config first, then default
	// local kubeconfig loading rules.
	Kubeconfig string

	// Context is optional and only applies when Kubeconfig is used.
	Context string

	Namespace string
	Pod       string
	Container string

	// Mode controls exec protocol preference.
	//
	// Supported:
	//   spdy
	//   websocket
	//   websocket-preferred
	//   raw-wss
	//
	// raw-wss uses WSSURL directly and does not require kubeconfig/client-go
	// executor construction.
	Mode string

	// Command is the remote command template. Supported placeholders:
	//   {{host}}
	//   {{port}}
	//   {{address}}
	//
	// Recommended:
	//   ["/usr/local/bin/kube-socks-relay", "tcp", "{{host}}", "{{port}}"]
	//
	// Debug only:
	//   ["socat", "-", "TCP:{{address}}"]
	Command []string

	// WSSURL is a direct Kubernetes pod exec WebSocket URL.
	//
	// Example:
	//   wss://api.example.com/api/v1/namespaces/ns/pods/pod/exec?container=relay
	//
	// If Command is set, command query parameters are injected automatically.
	// stdin/stdout/stderr/tty query parameters are also normalized.
	WSSURL string

	BearerToken     string
	BearerTokenFile string

	CAFile                string
	InsecureSkipTLSVerify bool
	ServerName            string

	Headers map[string]string
}

const (
	KubeExecModeSPDY               = "spdy"
	KubeExecModeWebSocket          = "websocket"
	KubeExecModeWebSocketPreferred = "websocket-preferred"
	KubeExecModeRawWSS             = "raw-wss"
)

// BuildRESTConfig creates a Kubernetes REST config.
//
// Resolution order:
//   - If kubeconfig path is supplied, load it.
//   - Otherwise try in-cluster config.
//   - If in-cluster fails, fall back to default kubeconfig loading rules.
func BuildRESTConfig(kubeconfigPath, contextName string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		loadingRules := &clientcmd.ClientConfigLoadingRules{
			ExplicitPath: kubeconfigPath,
		}

		overrides := &clientcmd.ConfigOverrides{
			CurrentContext: contextName,
		}

		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			overrides,
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfigPath, err)
		}

		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{
		CurrentContext: contextName,
	}

	cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		overrides,
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}

	return cfg, nil
}
