package ttl

import (
	"math"
	"sort"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

type Bucket struct {
	MaxSize int64
	TTL     time.Duration
	Label   string
}

type Policy struct {
	defaultTTL time.Duration
	buckets    []Bucket
}

func computeBuckets(cfg *config.Config) []Bucket {
	buckets := make([]Bucket, len(cfg.TTL.Table))

	for i, entry := range cfg.TTL.Table {
		maxSize := entry.MaxSize.Int64()
		if maxSize == -1 {
			maxSize = math.MaxInt64
		}
		label := computeLabel(maxSize)

		buckets[i] = Bucket{
			MaxSize: maxSize,
			TTL:     entry.TTL,
			Label:   label,
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].MaxSize < buckets[j].MaxSize
	})

	return buckets
}

func NewPolicy(cfg *config.Config) *Policy {
	return &Policy{
		defaultTTL: cfg.TTL.Default,
		buckets:    computeBuckets(cfg),
	}
}

func (p *Policy) ComputeTTL(sizeBytes int64) (time.Duration, string) {
	if sizeBytes <= 0 {
		return p.defaultTTL, "le_0"
	}

	for _, bucket := range p.buckets {
		if sizeBytes <= bucket.MaxSize {
			return bucket.TTL, bucket.Label
		}
	}

	return p.defaultTTL, "unknown"
}

func computeLabel(maxSize int64) string {
	if maxSize == math.MaxInt64 {
		return "gt_max"
	}
	return "le_" + humanizeBytes(maxSize)
}

func humanizeBytes(b int64) string {
	if b < 1024 {
		return itoa(b) + "B"
	}
	const unit = 1024
	values := []struct {
		suffix string
		factor int64
	}{
		{"KiB", unit},
		{"MiB", unit * unit},
		{"GiB", unit * unit * unit},
		{"TiB", unit * unit * unit * unit},
	}
	for i := len(values) - 1; i >= 0; i-- {
		if b >= values[i].factor {
			// round down to integer units for stable labels
			v := b / values[i].factor
			return itoa(v) + values[i].suffix
		}
	}
	return itoa(b) + "B"
}

// minimal int64 to string without fmt to avoid deps
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
