package filter

import (
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
)

type Filter struct {
	sizeLimit int64
}

func New(cfg *config.Config) *Filter {
	f := &Filter{}
	if cfg != nil {
		f.sizeLimit = cfg.Filter.SizeLimit.Int64()
	}
	monitor.SetFilterSizeLimit(f.sizeLimit)
	return f
}

func (f *Filter) ShouldFilter(size int64) bool {
	if f.sizeLimit <= 0 {
		monitor.ObserveFilter(size, false)
		return false
	}
	if size <= 0 {
		monitor.ObserveFilter(size, false)
		return false
	}
	filtered := size > f.sizeLimit
	monitor.ObserveFilter(size, filtered)
	return filtered
}

func (f *Filter) SizeLimit() int64 { return f.sizeLimit }
