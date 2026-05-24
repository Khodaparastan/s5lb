package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	return f.Name()
}

// TestLoadMultiFile_NoGroups verifies backward compatibility: a config without
// a `groups:` key loads as a MultiConfig with a single effective group.
func TestLoadMultiFile_NoGroups(t *testing.T) {
	path := writeYAML(t, `
listen: "127.0.0.1:1080"
upstreams:
  - host: proxy.example.com
    port: 1080
`)
	mc, err := LoadMultiFile(path)
	if err != nil {
		t.Fatalf("LoadMultiFile: %v", err)
	}
	if len(mc.Groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(mc.Groups))
	}
	effs := mc.EffectiveGroups()
	if len(effs) != 1 {
		t.Fatalf("expected 1 effective group, got %d", len(effs))
	}
	if effs[0].GroupName != "default" {
		t.Errorf("expected group name 'default', got %q", effs[0].GroupName)
	}
	if effs[0].ListenAddr != "127.0.0.1:1080" {
		t.Errorf("expected listen 127.0.0.1:1080, got %q", effs[0].ListenAddr)
	}
}

// TestLoadMultiFile_Groups verifies that groups are parsed and merged correctly.
func TestLoadMultiFile_Groups(t *testing.T) {
	path := writeYAML(t, `
listen: "127.0.0.1:9999"  # global default (not used directly)
strategy: least-active
health_interval: 20s
upstreams:
  - host: global-proxy.example.com
    port: 1080

groups:
  - name: web
    listen: "127.0.0.1:1080"
    strategy: round-robin
    upstreams:
      - host: web-proxy.example.com
        port: 1080
  - name: db
    listen: "127.0.0.1:1081"
    connect_timeout: 3s
    upstreams:
      - host: db-proxy.example.com
        port: 1080
`)
	mc, err := LoadMultiFile(path)
	if err != nil {
		t.Fatalf("LoadMultiFile: %v", err)
	}
	if len(mc.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(mc.Groups))
	}

	effs := mc.EffectiveGroups()
	if len(effs) != 2 {
		t.Fatalf("expected 2 effective groups, got %d", len(effs))
	}

	// Group "web": overrides listen and strategy.
	web := effs[0]
	if web.GroupName != "web" {
		t.Errorf("web: expected name 'web', got %q", web.GroupName)
	}
	if web.ListenAddr != "127.0.0.1:1080" {
		t.Errorf("web: expected listen 127.0.0.1:1080, got %q", web.ListenAddr)
	}
	if web.Strategy != "round-robin" {
		t.Errorf("web: expected strategy round-robin, got %q", web.Strategy)
	}
	if web.HealthInterval != 20*time.Second {
		t.Errorf("web: expected health_interval 20s (inherited), got %v", web.HealthInterval)
	}
	if len(web.Upstreams) != 1 || web.Upstreams[0].Host != "web-proxy.example.com" {
		t.Errorf("web: unexpected upstreams %+v", web.Upstreams)
	}

	// Group "db": overrides listen and connect_timeout; inherits strategy.
	db := effs[1]
	if db.GroupName != "db" {
		t.Errorf("db: expected name 'db', got %q", db.GroupName)
	}
	if db.ListenAddr != "127.0.0.1:1081" {
		t.Errorf("db: expected listen 127.0.0.1:1081, got %q", db.ListenAddr)
	}
	if db.Strategy != "least-active" {
		t.Errorf("db: expected inherited strategy least-active, got %q", db.Strategy)
	}
	if db.ConnectTimeout != 3*time.Second {
		t.Errorf("db: expected connect_timeout 3s, got %v", db.ConnectTimeout)
	}
}

// TestMultiConfig_Validate_DuplicateNames checks that duplicate group names are rejected.
func TestMultiConfig_Validate_DuplicateNames(t *testing.T) {
	mc := MultiConfig{
		Config: Defaults(),
		Groups: []GroupConfig{
			{
				Name:      "alpha",
				Listen:    "127.0.0.1:1080",
				Upstreams: []UpstreamSpec{{Host: "h1", Port: 1080}},
			},
			{
				Name:      "alpha",
				Listen:    "127.0.0.1:1081",
				Upstreams: []UpstreamSpec{{Host: "h2", Port: 1080}},
			},
		},
	}
	if err := mc.Validate(); err == nil {
		t.Error("expected error for duplicate group name, got nil")
	}
}

// TestMultiConfig_Validate_MissingName checks that an unnamed group is rejected.
func TestMultiConfig_Validate_MissingName(t *testing.T) {
	mc := MultiConfig{
		Config: Defaults(),
		Groups: []GroupConfig{
			{Listen: "127.0.0.1:1080", Upstreams: []UpstreamSpec{{Host: "h1", Port: 1080}}},
		},
	}
	if err := mc.Validate(); err == nil {
		t.Error("expected error for missing group name, got nil")
	}
}

// TestMultiConfig_Validate_InvalidGroup checks that a group with invalid config is rejected.
func TestMultiConfig_Validate_InvalidGroup(t *testing.T) {
	zero := 0
	mc := MultiConfig{
		Config: Defaults(),
		Groups: []GroupConfig{
			{
				Name:        "bad",
				Listen:      "127.0.0.1:1080",
				MaxPerProxy: &zero, // invalid: must be > 0
				Upstreams:   []UpstreamSpec{{Host: "h1", Port: 1080}},
			},
		},
	}
	if err := mc.Validate(); err == nil {
		t.Error("expected error for invalid group config, got nil")
	}
}

// TestMultiConfig_GroupNames verifies group name listing.
func TestMultiConfig_GroupNames(t *testing.T) {
	t.Run("no groups", func(t *testing.T) {
		mc := MultiConfig{Config: Defaults()}
		names := mc.GroupNames()
		if len(names) != 1 || names[0] != "default" {
			t.Errorf("expected [default], got %v", names)
		}
	})

	t.Run("with groups", func(t *testing.T) {
		mc := MultiConfig{
			Config: Defaults(),
			Groups: []GroupConfig{
				{Name: "alpha"},
				{Name: "beta"},
			},
		}
		names := mc.GroupNames()
		if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
			t.Errorf("expected [alpha beta], got %v", names)
		}
	})
}

// TestLoadMultiFile_NonExistent checks error handling for missing files.
func TestLoadMultiFile_NonExistent(t *testing.T) {
	_, err := LoadMultiFile(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
}

// TestLoadMultiFile_BackwardCompat_Validate verifies that a single-group config
// loaded via LoadMultiFile still passes Validate() unchanged.
func TestLoadMultiFile_BackwardCompat_Validate(t *testing.T) {
	path := writeYAML(t, `
listen: "127.0.0.1:1080"
upstreams:
  - host: proxy.example.com
    port: 1080
`)
	mc, err := LoadMultiFile(path)
	if err != nil {
		t.Fatalf("LoadMultiFile: %v", err)
	}
	if err := mc.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// TestMultiConfig_EffectiveGroups_GlobalFallback verifies that a group with
// no upstreams falls back to the global upstreams.
func TestMultiConfig_EffectiveGroups_GlobalFallback(t *testing.T) {
	mc := MultiConfig{
		Config: Defaults(),
		Groups: []GroupConfig{
			{Name: "fallback", Listen: "127.0.0.1:1080"},
		},
	}
	mc.Config.Upstreams = []UpstreamSpec{{Host: "global", Port: 1080}}

	effs := mc.EffectiveGroups()
	if len(effs) != 1 {
		t.Fatalf("expected 1 effective group, got %d", len(effs))
	}
	if len(effs[0].Upstreams) != 1 || effs[0].Upstreams[0].Host != "global" {
		t.Errorf("expected global upstream fallback, got %+v", effs[0].Upstreams)
	}
}
