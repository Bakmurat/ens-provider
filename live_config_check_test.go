package main

// Release pre-flight harness: validates whatever provider env the caller
// exported — i.e. the LIVE rendered cluster config (kustomize-build the fleet
// overlay, helm-template the chart, export the ConfigMap) — against the
// current LoadConfig, so a new or tightened startup validation is proven
// non-breaking BEFORE the image ships (first used for the P0-1 label-syntax
// gate, 2026-08). Skipped unless LIVE_CONFIG_CHECK=1; zero suite cost.

import (
	"os"
	"testing"
)

func TestLiveConfigFromEnv(t *testing.T) {
	if os.Getenv("LIVE_CONFIG_CHECK") == "" {
		t.Skip("set LIVE_CONFIG_CHECK=1 with the real provider env exported")
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LIVE config would be REJECTED at provider boot: %v", err)
	}
	t.Logf("live config OK: %d groups", len(cfg.Groups))
	for _, g := range cfg.Groups {
		t.Logf("  group %s prefix=%s labels=%d taints=%d", g.ID, g.NamePrefix, len(g.Labels), len(g.Taints))
	}
}
