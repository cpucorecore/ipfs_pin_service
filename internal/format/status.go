package format

import (
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

// TranslateStatus 将状态码转换为可读字符串
func TranslateStatus(status store.Status) string {
	switch status {
	case store.StatusActive:
		return "Active"
	case store.StatusPinning:
		return "Pinning"
	case store.StatusPinSucceeded:
		return "PinSucceeded"
	case store.StatusPinFailed:
		return "PinFailed"
	case store.StatusScheduledForUnpin:
		return "ScheduledForUnpin"
	case store.StatusUnpinning:
		return "Unpinning"
	case store.StatusUnpinSucceeded:
		return "UnpinSucceeded"
	case store.StatusDeadLetter:
		return "DeadLetter"
	default:
		return "Unknown"
	}
}
