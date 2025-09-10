package filter

import (
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
)

type SizeFilter struct {
	sizeLimit int64
}

func NewSizeFilter(sizeLimit int64) *SizeFilter {
	f := &SizeFilter{
		sizeLimit: sizeLimit,
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
