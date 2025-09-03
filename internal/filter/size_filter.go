package filter

import (
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
)

type SizeFilter struct {
	sizeLimit int64
}

func NewSizeFilter(cfg *config.Config) *SizeFilter {
	f := &SizeFilter{}
	if cfg != nil {
		f.sizeLimit = cfg.Filter.SizeLimit.Int64()
	}
	monitor.SetFilterSizeLimit(f.sizeLimit)
	return f
}

func (f *SizeFilter) ShouldFilter(size int64) bool {
	if f.sizeLimit <= 0 {
		monitor.ObserveFilter(false)
		return false
	}
	if size <= 0 {
		monitor.ObserveFilter(false)
		return false
	}
	filtered := size > f.sizeLimit
	monitor.ObserveFilter(filtered)
	return filtered
}

func (f *SizeFilter) SizeLimit() int64 { return f.sizeLimit }
