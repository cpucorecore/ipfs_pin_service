package ttl

import (
	"sort"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

type Policy struct {
	defaultTTL time.Duration
	table      []struct {
		maxSize int64
		ttl     time.Duration
	}
}

func NewPolicy(cfg *config.Config) *Policy {
	table := make([]struct {
		maxSize int64
		ttl     time.Duration
	}, len(cfg.TTL.Table))

	for i, entry := range cfg.TTL.Table {
		table[i] = struct {
			maxSize int64
			ttl     time.Duration
		}{
			maxSize: entry.MaxSize.Int64(),
			ttl:     entry.TTL,
		}
	}

	// Sort by maxSize ascending, but treat -1 as infinity and place it at the end
	sort.Slice(table, func(i, j int) bool {
		li := table[i].maxSize
		lj := table[j].maxSize
		if li == -1 {
			return false
		}
		if lj == -1 {
			return true
		}
		return li < lj
	})

	return &Policy{
		defaultTTL: cfg.TTL.Default,
		table:      table,
	}
}

func (p *Policy) Compute(sizeBytes int64) time.Duration {
	if sizeBytes <= 0 {
		return p.defaultTTL
	}

	// Match first entry where size <= maxSize, skipping the catch-all (-1)
	for _, entry := range p.table {
		if entry.maxSize != -1 && sizeBytes <= entry.maxSize {
			return entry.ttl
		}
	}

	// Fallback: if the last entry is catch-all (-1), use it; otherwise use last tier
	if len(p.table) > 0 {
		last := p.table[len(p.table)-1]
		return last.ttl
	}
	return p.defaultTTL
}

// ComputeWithBucket returns TTL and a human-friendly bucket label for metrics.
// Buckets are derived from policy table thresholds; the catch-all (-1) is labeled as "gt_max".
func (p *Policy) ComputeWithBucket(sizeBytes int64) (time.Duration, string) {
	if sizeBytes <= 0 {
		return p.defaultTTL, "le_0"
	}

	// Match first non-(-1) threshold
	for _, entry := range p.table {
		if entry.maxSize != -1 && sizeBytes <= entry.maxSize {
			return entry.ttl, "le_" + humanizeBytes(entry.maxSize)
		}
	}

	// Fallback to last bucket
	if len(p.table) > 0 {
		last := p.table[len(p.table)-1]
		if last.maxSize == -1 {
			return last.ttl, "gt_max"
		}
		return last.ttl, "le_" + humanizeBytes(last.maxSize)
	}
	return p.defaultTTL, "unknown"
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
