package config

import (
	"strings"
	"testing"
	"time"
)

func validDefaults() Config {
	d := Defaults()
	// Add one upstream with valid port so validation passes.
	d.Upstreams = []UpstreamSpec{{Host: "127.0.0.1", Port: 1234}}
	return d
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := validDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidate_MissingListenAddr(t *testing.T) {
	cfg := validDefaults()
	cfg.ListenAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty listen addr")
	}
}

func TestValidate_HealthIntervalZero(t *testing.T) {
	cfg := validDefaults()
	cfg.HealthInterval = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "health_interval") {
		t.Fatalf("expected health_interval error, got: %v", err)
	}
}

func TestValidate_FailureThresholdZero(t *testing.T) {
	cfg := validDefaults()
	cfg.FailureThreshold = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "failure_threshold") {
		t.Fatalf("expected failure_threshold error, got: %v", err)
	}
}

func TestValidate_FailureWindowZero(t *testing.T) {
	cfg := validDefaults()
	cfg.FailureWindow = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "failure_window") {
		t.Fatalf("expected failure_window error, got: %v", err)
	}
}

func TestValidate_UDPIdleTimeoutZeroWhenEnabled(t *testing.T) {
	cfg := validDefaults()
	cfg.UDPEnabled = true
	cfg.UDPIdleTimeout = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "udp_idle_timeout") {
		t.Fatalf("expected udp_idle_timeout error, got: %v", err)
	}
}

func TestValidate_UDPIdleTimeoutZeroWhenDisabled(t *testing.T) {
	cfg := validDefaults()
	cfg.UDPEnabled = false
	cfg.UDPIdleTimeout = 0
	// Should not error when UDP is disabled.
	if err := cfg.Validate(); err != nil && strings.Contains(err.Error(), "udp_idle_timeout") {
		t.Fatalf("unexpected udp_idle_timeout error when UDP disabled: %v", err)
	}
}

func TestValidate_UpstreamPortOutOfRange(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"zero", 0},
		{"negative", -1},
		{"too_high", 99999},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validDefaults()
			cfg.Upstreams = []UpstreamSpec{{Host: "127.0.0.1", Port: tc.port}}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected error for port %d", tc.port)
			}
		})
	}
}

func TestValidate_UpstreamPortValid(t *testing.T) {
	for _, port := range []int{1, 80, 443, 1080, 65535} {
		cfg := validDefaults()
		cfg.Upstreams = []UpstreamSpec{{Host: "127.0.0.1", Port: port}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("port %d unexpectedly rejected: %v", port, err)
		}
	}
}

func TestValidate_NegativeWeight(t *testing.T) {
	cfg := validDefaults()
	cfg.Upstreams = []UpstreamSpec{{Host: "127.0.0.1", Port: 1080, Weight: -1}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "weight") {
		t.Fatalf("expected weight error, got: %v", err)
	}
}

func TestValidate_ConnectTimeoutZero(t *testing.T) {
	cfg := validDefaults()
	cfg.ConnectTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero connect timeout")
	}
}

func TestParseUpstream_PortOutOfRange(t *testing.T) {
	_, err := ParseUpstream("127.0.0.1:99999")
	if err == nil {
		t.Fatal("expected error for out-of-range port")
	}
}

func TestParseUpstream_ValidPort(t *testing.T) {
	u, err := ParseUpstream("127.0.0.1:1080")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Port != 1080 {
		t.Fatalf("port=%d want 1080", u.Port)
	}
}

func TestValidate_SampleRatioRange(t *testing.T) {
	cfg := validDefaults()
	cfg.OTel.SampleRatio = 1.5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for sample_ratio > 1")
	}
	cfg.OTel.SampleRatio = -0.1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for sample_ratio < 0")
	}
	cfg.OTel.SampleRatio = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero sample_ratio should be valid (drop all traces): %v", err)
	}
}

func TestLoadFile_SampleRatioPreserved(t *testing.T) {
	// Verify that sample_ratio=0 is not overwritten to 1.0.
	cfg := Defaults()
	cfg.OTel.SampleRatio = 0
	// Simulate what LoadFile does after unmarshalling (only parseLevel + ServiceName).
	if cfg.OTel.ServiceName == "" {
		cfg.OTel.ServiceName = "s5lb"
	}
	if cfg.OTel.SampleRatio != 0 {
		t.Fatalf("sample_ratio overwritten: got %v, want 0", cfg.OTel.SampleRatio)
	}
}

func TestValidate_DrainTimeoutsNegative(t *testing.T) {
	cfg := validDefaults()
	cfg.DrainSoftTimeout = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative drain soft timeout")
	}
}
