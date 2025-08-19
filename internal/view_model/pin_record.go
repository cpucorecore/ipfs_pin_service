package view_model

import (
	"fmt"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

type PinRecordView struct {
	CID               string           `json:"cid"`
	Status            string           `json:"status"`
	ReceivedAt        string           `json:"received_at"`
	EnqueuedAt        string           `json:"enqueued_at,omitempty"`
	PinStartAt        string           `json:"pin_start_at,omitempty"`
	PinSucceededAt    string           `json:"pin_succeeded_at,omitempty"`
	ExpireAt          string           `json:"expire_at,omitempty"`
	ScheduleUnpinAt   string           `json:"schedule_unpin_at,omitempty"`
	UnpinStartAt      string           `json:"unpin_start_at,omitempty"`
	UnpinSucceededAt  string           `json:"unpin_succeeded_at,omitempty"`
	LastUpdateAt      string           `json:"last_update_at"`
	Size              int64            `json:"size"`
	SizeHuman         string           `json:"size_human"`
	PinAttemptCount   int32            `json:"pin_attempt_count"`
	UnpinAttemptCount int32            `json:"unpin_attempt_count"`
	TTL               string           `json:"ttl,omitempty"`
	Age               string           `json:"age"`
	History           []*PinRecordView `json:"history,omitempty"` // 历史记录
}

func ConvertPinRecord(r *store.PinRecord, timeFormat TimeFormat) *PinRecordView {
	now := time.Now()
	receivedAt := time.UnixMilli(r.ReceivedAt)

	view := &PinRecordView{
		CID:               r.Cid,
		Status:            TranslateStatus(r.Status),
		ReceivedAt:        FormatTime(receivedAt, timeFormat),
		LastUpdateAt:      FormatTime(time.UnixMilli(r.LastUpdateAt), timeFormat),
		Size:              r.Size,
		SizeHuman:         FormatBytes(r.Size),
		PinAttemptCount:   r.PinAttemptCount,
		UnpinAttemptCount: r.UnpinAttemptCount,
	}

	if r.EnqueuedAt > 0 {
		t := time.UnixMilli(r.EnqueuedAt)
		view.EnqueuedAt = FormatTime(t, timeFormat)
	}
	if r.PinStartAt > 0 {
		t := time.UnixMilli(r.PinStartAt)
		view.PinStartAt = FormatTime(t, timeFormat)
	}
	if r.PinSucceededAt > 0 {
		t := time.UnixMilli(r.PinSucceededAt)
		view.PinSucceededAt = FormatTime(t, timeFormat)
	}
	if r.ExpireAt > 0 {
		t := time.UnixMilli(r.ExpireAt)
		view.ExpireAt = FormatTime(t, timeFormat)
		if now.Before(t) {
			view.TTL = FormatDuration(t.Sub(now))
		} else {
			view.TTL = "expired"
		}
	}
	if r.ScheduleUnpinAt > 0 {
		t := time.UnixMilli(r.ScheduleUnpinAt)
		view.ScheduleUnpinAt = FormatTime(t, timeFormat)
	}
	if r.UnpinStartAt > 0 {
		t := time.UnixMilli(r.UnpinStartAt)
		view.UnpinStartAt = FormatTime(t, timeFormat)
	}
	if r.UnpinSucceededAt > 0 {
		t := time.UnixMilli(r.UnpinSucceededAt)
		view.UnpinSucceededAt = FormatTime(t, timeFormat)
	}

	// Age display depends on status:
	// - Pinning: duration since PinStartAt
	// - Unpinning: duration since UnpinStartAt
	// - Active/PinSucceeded: duration since PinSucceededAt
	// - others: empty
	switch r.Status {
	case store.StatusPinning:
		if r.PinStartAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.PinStartAt)))
		}
	case store.StatusUnpinning:
		if r.UnpinStartAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.UnpinStartAt)))
		}
	case store.StatusActive, store.StatusPinSucceeded:
		if r.PinSucceededAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.PinSucceededAt)))
		}
	case store.StatusFiltered:
		view.Age = fmt.Sprintf("Filtered due to size limit: %s", FormatBytes(r.SizeLimit))
	default:
		view.Age = ""
	}

	// 处理历史记录（不包含历史记录本身，避免递归）
	if r.History != nil && len(r.History) > 0 {
		view.History = make([]*PinRecordView, len(r.History))
		for i, historyRecord := range r.History {
			view.History[i] = convertPinRecordWithoutHistory(historyRecord, timeFormat)
		}
	}

	return view
}

// convertPinRecordWithoutHistory 转换 PinRecord 但不包含历史记录，避免递归
func convertPinRecordWithoutHistory(r *store.PinRecord, timeFormat TimeFormat) *PinRecordView {
	now := time.Now()
	receivedAt := time.UnixMilli(r.ReceivedAt)

	view := &PinRecordView{
		CID:               r.Cid,
		Status:            TranslateStatus(r.Status),
		ReceivedAt:        FormatTime(receivedAt, timeFormat),
		LastUpdateAt:      FormatTime(time.UnixMilli(r.LastUpdateAt), timeFormat),
		Size:              r.Size,
		SizeHuman:         FormatBytes(r.Size),
		PinAttemptCount:   r.PinAttemptCount,
		UnpinAttemptCount: r.UnpinAttemptCount,
		// 注意：不包含 History 字段
	}

	if r.EnqueuedAt > 0 {
		t := time.UnixMilli(r.EnqueuedAt)
		view.EnqueuedAt = FormatTime(t, timeFormat)
	}
	if r.PinStartAt > 0 {
		t := time.UnixMilli(r.PinStartAt)
		view.PinStartAt = FormatTime(t, timeFormat)
	}
	if r.PinSucceededAt > 0 {
		t := time.UnixMilli(r.PinSucceededAt)
		view.PinSucceededAt = FormatTime(t, timeFormat)
	}
	if r.ExpireAt > 0 {
		t := time.UnixMilli(r.ExpireAt)
		view.ExpireAt = FormatTime(t, timeFormat)
		if now.Before(t) {
			view.TTL = FormatDuration(t.Sub(now))
		} else {
			view.TTL = "expired"
		}
	}
	if r.ScheduleUnpinAt > 0 {
		t := time.UnixMilli(r.ScheduleUnpinAt)
		view.ScheduleUnpinAt = FormatTime(t, timeFormat)
	}
	if r.UnpinStartAt > 0 {
		t := time.UnixMilli(r.UnpinStartAt)
		view.UnpinStartAt = FormatTime(t, timeFormat)
	}
	if r.UnpinSucceededAt > 0 {
		t := time.UnixMilli(r.UnpinSucceededAt)
		view.UnpinSucceededAt = FormatTime(t, timeFormat)
	}

	// Age display depends on status:
	// - Pinning: duration since PinStartAt
	// - Unpinning: duration since UnpinStartAt
	// - Active/PinSucceeded: duration since PinSucceededAt
	// - others: empty
	switch r.Status {
	case store.StatusPinning:
		if r.PinStartAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.PinStartAt)))
		}
	case store.StatusUnpinning:
		if r.UnpinStartAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.UnpinStartAt)))
		}
	case store.StatusActive, store.StatusPinSucceeded:
		if r.PinSucceededAt > 0 {
			view.Age = FormatDuration(now.Sub(time.UnixMilli(r.PinSucceededAt)))
		}
	case store.StatusFiltered:
		view.Age = fmt.Sprintf("Filtered due to size limit: %s", FormatBytes(r.SizeLimit))
	default:
		view.Age = ""
	}

	return view
}

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
	case store.StatusFiltered:
		return "Filtered"
	default:
		return "Unknown"
	}
}

func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	parts := []string{}
	if d >= time.Hour {
		hours := d / time.Hour
		d -= hours * time.Hour
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if d >= time.Minute {
		minutes := d / time.Minute
		d -= minutes * time.Minute
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	if d >= time.Second {
		seconds := d / time.Second
		parts = append(parts, fmt.Sprintf("%ds", seconds))
	}
	return parts[0]
}
