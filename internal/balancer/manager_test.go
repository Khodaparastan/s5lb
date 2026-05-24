package balancer

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/khodaparastan/s5lb/internal/config"
	"github.com/khodaparastan/s5lb/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace/noop"
)

func testReg(t *testing.T) prometheus.Registerer {
	t.Helper()
	return prometheus.NewRegistry()
}

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// freePort returns a random available TCP port on loopback.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// makeMultiConfig builds a simple two-group MultiConfig for testing.
func makeMultiConfig(t *testing.T) config.MultiConfig {
	t.Helper()
	p1 := freePort(t)
	p2 := freePort(t)
	mc := config.MultiConfig{Config: config.Defaults()}
	mc.Config.ListenAddr = "127.0.0.1:0" // not used in multi-group mode
	mc.Groups = []config.GroupConfig{
		{
			Name:   "alpha",
			Listen: "127.0.0.1:" + itoa(p1),
			Upstreams: []config.UpstreamSpec{
				{Host: "127.0.0.1", Port: 9001},
			},
		},
		{
			Name:   "beta",
			Listen: "127.0.0.1:" + itoa(p2),
			Upstreams: []config.UpstreamSpec{
				{Host: "127.0.0.1", Port: 9002},
			},
		},
	}
	return mc
}

func itoa(i int) string {
	return string([]byte(sprintf(i)))
}

func sprintf(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

// TestNewManager_SingleGroup verifies backward-compat single-group creation.
func TestNewManager_SingleGroup(t *testing.T) {
	p := freePort(t)
	mc := config.MultiConfig{Config: config.Defaults()}
	mc.Config.ListenAddr = "127.0.0.1:" + sprintf(p)
	mc.Config.Upstreams = []config.UpstreamSpec{{Host: "127.0.0.1", Port: 9001}}

	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if len(mgr.Groups()) != 1 {
		t.Errorf("expected 1 group, got %d", len(mgr.Groups()))
	}
	if g := mgr.Group("default"); g == nil {
		t.Error("expected group 'default' to exist")
	}
}

// TestNewManager_MultiGroup verifies that multiple groups are created.
func TestNewManager_MultiGroup(t *testing.T) {
	mc := makeMultiConfig(t)
	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if len(mgr.Groups()) != 2 {
		t.Errorf("expected 2 groups, got %d", len(mgr.Groups()))
	}
	if mgr.Group("alpha") == nil {
		t.Error("expected group 'alpha'")
	}
	if mgr.Group("beta") == nil {
		t.Error("expected group 'beta'")
	}
	if mgr.Group("nonexistent") != nil {
		t.Error("expected nil for unknown group")
	}
}

// TestManager_Shutdown drains all groups without blocking.
func TestManager_Shutdown(t *testing.T) {
	mc := makeMultiConfig(t)
	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = mgr.Start(context.Background())
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	mgr.Shutdown()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Manager.Start did not return after Shutdown")
	}
}

// TestManager_GroupInfos verifies GroupInfos returns entries for all groups.
func TestManager_GroupInfos(t *testing.T) {
	mc := makeMultiConfig(t)
	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	infos := mgr.GroupInfos()
	if infos == nil {
		t.Fatal("GroupInfos returned nil")
	}
	// Must be a slice of GroupInfo
	slice, ok := infos.([]GroupInfo)
	if !ok {
		t.Fatalf("expected []GroupInfo, got %T", infos)
	}
	if len(slice) != 2 {
		t.Errorf("expected 2 group infos, got %d", len(slice))
	}
	names := map[string]bool{}
	for _, gi := range slice {
		names[gi.Name] = true
	}
	if !names["alpha"] || !names["beta"] {
		t.Errorf("missing expected group names in infos: %v", names)
	}
}

// TestManager_GroupInfoByName verifies per-group lookup.
func TestManager_GroupInfoByName(t *testing.T) {
	mc := makeMultiConfig(t)
	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	info, ok := mgr.GroupInfoByName("alpha")
	if !ok {
		t.Fatal("expected to find group 'alpha'")
	}
	gi := info.(GroupInfo)
	if gi.Name != "alpha" {
		t.Errorf("expected name 'alpha', got %q", gi.Name)
	}

	_, ok = mgr.GroupInfoByName("nonexistent")
	if ok {
		t.Error("expected false for nonexistent group")
	}
}

// TestManager_ReloadGroup_NotFound checks error on unknown group reload.
func TestManager_ReloadGroup_NotFound(t *testing.T) {
	mc := makeMultiConfig(t)
	reg := testReg(t)
	if err := metrics.RegisterBuildInfo(reg, "test", "test", "test"); err != nil {
		t.Fatalf("RegisterBuildInfo: %v", err)
	}
	mgr, err := NewManager(
		mc,
		"",
		testLog(),
		reg,
		noop.NewTracerProvider().Tracer("t"),
		"test",
		"test",
		"test",
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.ReloadGroup("nonexistent"); err == nil {
		t.Error("expected error for unknown group, got nil")
	}
}
