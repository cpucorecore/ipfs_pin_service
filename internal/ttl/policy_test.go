package ttl

import (
	"testing"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

func buildConfig() *config.Config {
	cfg := &config.Config{}
	// default TTL
	cfg.TTL.Default = time.Minute
	// Intentionally out of order and containing -1 to verify sorting/handling
	cfg.TTL.Table = []struct {
		MaxSize config.FileSize `yaml:"max_size"`
		TTL     time.Duration   `yaml:"ttl"`
	}{
		{MaxSize: config.FileSize(10 * 1024 * 1024), TTL: 45 * time.Second},  // 10M -> 45s
		{MaxSize: config.FileSize(-1), TTL: 15 * time.Second},                // catch-all -> 15s
		{MaxSize: config.FileSize(100 * 1024 * 1024), TTL: 30 * time.Second}, // 100M -> 30s
		{MaxSize: config.FileSize(500 * 1024), TTL: 60 * time.Second},        // 500K -> 60s
	}
	return cfg
}

func TestPolicyCompute(t *testing.T) {
	cfg := buildConfig()
	p := NewPolicy(cfg)

	// size <= 0 -> default
	if got := p.Compute(0); got != time.Minute {
		t.Fatalf("size<=0 expected default 1m, got %v", got)
	}

	// <= 500K -> 60s
	if got := p.Compute(500 * 1024); got != 60*time.Second {
		t.Fatalf("<=500K expected 60s, got %v", got)
	}

	// 1MB -> 45s (<=10M)
	if got := p.Compute(1 * 1024 * 1024); got != 45*time.Second {
		t.Fatalf("1MB expected 45s, got %v", got)
	}

	// 50MB -> 30s (<=100M)
	if got := p.Compute(50 * 1024 * 1024); got != 30*time.Second {
		t.Fatalf("50MB expected 30s, got %v", got)
	}

	// 1GB -> 15s (catch-all -1)
	if got := p.Compute(1024 * 1024 * 1024); got != 15*time.Second {
		t.Fatalf("1GB expected 15s, got %v", got)
	}
}
