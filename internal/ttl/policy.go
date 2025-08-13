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
