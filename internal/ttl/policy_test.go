package ttl

import (
	"math"
	"testing"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

func TestComputeBuckets(t *testing.T) {
	cfg := &config.Config{}
	cfg.TTL.Default = 24 * time.Hour
	cfg.TTL.Table = []struct {
		MaxSize config.FileSize `yaml:"max_size"`
		TTL     time.Duration   `yaml:"ttl"`
	}{
		{MaxSize: config.FileSize(1024 * 1024), TTL: 6 * time.Hour}, // 1MiB
		{MaxSize: config.FileSize(-1), TTL: 24 * time.Hour},         // unlimited
		{MaxSize: config.FileSize(1024), TTL: 1 * time.Hour},        // 1KiB
	}

	buckets := computeBuckets(cfg)

	// 测试bucket数量
	if len(buckets) != 3 {
		t.Errorf("期望3个bucket，实际得到 %d 个", len(buckets))
	}

	// 测试排序是否正确（应该按MaxSize升序排列）
	expectedSizes := []int64{1024, 1024 * 1024, math.MaxInt64}
	for i, bucket := range buckets {
		if bucket.MaxSize != expectedSizes[i] {
			t.Errorf("Bucket %d: 期望MaxSize %d，实际得到 %d", i, expectedSizes[i], bucket.MaxSize)
		}
	}

	// 测试标签是否正确计算
	expectedLabels := []string{"le_1KiB", "le_1MiB", "gt_max"}
	for i, bucket := range buckets {
		if bucket.Label != expectedLabels[i] {
			t.Errorf("Bucket %d: 期望标签 %s，实际得到 %s", i, expectedLabels[i], bucket.Label)
		}
	}

	// 测试TTL是否正确设置
	expectedTTLs := []time.Duration{1 * time.Hour, 6 * time.Hour, 24 * time.Hour}
	for i, bucket := range buckets {
		if bucket.TTL != expectedTTLs[i] {
			t.Errorf("Bucket %d: 期望TTL %v，实际得到 %v", i, expectedTTLs[i], bucket.TTL)
		}
	}
}

func TestNewPolicy(t *testing.T) {
	cfg := &config.Config{}
	cfg.TTL.Default = 24 * time.Hour
	cfg.TTL.Table = []struct {
		MaxSize config.FileSize `yaml:"max_size"`
		TTL     time.Duration   `yaml:"ttl"`
	}{
		{MaxSize: config.FileSize(1024), TTL: 1 * time.Hour},        // 1KiB
		{MaxSize: config.FileSize(1024 * 1024), TTL: 6 * time.Hour}, // 1MiB
		{MaxSize: config.FileSize(-1), TTL: 24 * time.Hour},         // unlimited
	}

	policy := NewPolicy(cfg)

	// Test that buckets are created with pre-computed labels
	if len(policy.buckets) != 3 {
		t.Errorf("Expected 3 buckets, got %d", len(policy.buckets))
	}

	// Check that labels are pre-computed
	expectedLabels := []string{"le_1KiB", "le_1MiB", "gt_max"}
	for i, bucket := range policy.buckets {
		if bucket.Label != expectedLabels[i] {
			t.Errorf("Bucket %d: expected label %s, got %s", i, expectedLabels[i], bucket.Label)
		}
	}

	// Test TTL computation
	tests := []struct {
		size     int64
		expected time.Duration
		label    string
	}{
		{512, 1 * time.Hour, "le_1KiB"},             // Small file
		{1024, 1 * time.Hour, "le_1KiB"},            // Exactly 1KiB
		{2048, 6 * time.Hour, "le_1MiB"},            // Between 1KiB and 1MiB
		{1024 * 1024, 6 * time.Hour, "le_1MiB"},     // Exactly 1MiB
		{2 * 1024 * 1024, 24 * time.Hour, "gt_max"}, // Larger than 1MiB
	}

	for _, test := range tests {
		ttl, label := policy.ComputeTTL(test.size)
		if ttl != test.expected {
			t.Errorf("Size %d: expected TTL %v, got %v", test.size, test.expected, ttl)
		}
		if label != test.label {
			t.Errorf("Size %d: expected label %s, got %s", test.size, test.label, label)
		}
	}
}

func TestComputeLabel(t *testing.T) {
	tests := []struct {
		maxSize  int64
		expected string
	}{
		{math.MaxInt64, "gt_max"},
		{1024, "le_1KiB"},
		{1024 * 1024, "le_1MiB"},
		{1024 * 1024 * 1024, "le_1GiB"},
		{512, "le_512B"},
	}

	for _, test := range tests {
		label := computeLabel(test.maxSize)
		if label != test.expected {
			t.Errorf("MaxSize %d: expected label %s, got %s", test.maxSize, test.expected, label)
		}
	}
}
