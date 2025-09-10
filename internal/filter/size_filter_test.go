package filter

import (
	"testing"
)

func TestShouldFilter_SizeLimitZero_NoFilter(t *testing.T) {
	f := NewSizeFilter(0)
	if f.ShouldFilter(0) {
		t.Fatalf("expected no filter when sizeLimit=0 and size=0")
	}
	if f.ShouldFilter(123) {
		t.Fatalf("expected no filter when sizeLimit=0 and size>0")
	}
}

func TestShouldFilter_SizeUnknown_NoFilter(t *testing.T) {
	f := NewSizeFilter(100)
	if f.ShouldFilter(0) {
		t.Fatalf("expected no filter when size is 0 (unknown)")
	}
}

func TestShouldFilter_GreaterThanLimit_Filtered(t *testing.T) {
	f := NewSizeFilter(100)
	if !f.ShouldFilter(101) {
		t.Fatalf("expected filter when size > limit")
	}
	if f.SizeLimit() != 100 {
		t.Fatalf("unexpected size limit, got %d", f.SizeLimit())
	}
}

func TestShouldFilter_EqualOrLess_NoFilter(t *testing.T) {
	f := NewSizeFilter(100)
	if f.ShouldFilter(100) {
		t.Fatalf("expected no filter when size == limit")
	}
	if f.ShouldFilter(99) {
		t.Fatalf("expected no filter when size < limit")
	}
}
