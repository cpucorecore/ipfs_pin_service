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

	// 按大小排序
	sort.Slice(table, func(i, j int) bool {
		return table[i].maxSize < table[j].maxSize
	})

	return &Policy{
		defaultTTL: cfg.TTL.Default,
		table:      table,
	}
}

func (p *Policy) Compute(sizeBytes int64) time.Duration {
	// 如果大小为 0 或负数，使用默认 TTL
	if sizeBytes <= 0 {
		return p.defaultTTL
	}

	// 否则使用基于大小的策略
	for _, entry := range p.table {
		if entry.maxSize == -1 || sizeBytes <= entry.maxSize {
			return entry.ttl
		}
	}

	// 如果没有匹配的规则，使用最后一个规则的 TTL
	return p.table[len(p.table)-1].ttl
}
