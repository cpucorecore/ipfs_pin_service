package format

import (
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

// TranslateStatus converts the numeric status to a readable string
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
