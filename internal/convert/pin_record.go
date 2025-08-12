package convert

import (
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/format"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

// PinRecordView is the response view model for a pin record
type PinRecordView struct {
	CID               string `json:"cid"`
	Status            string `json:"status"`
	ReceivedAt        string `json:"received_at"`
	EnqueuedAt        string `json:"enqueued_at,omitempty"`
	PinStartAt        string `json:"pin_start_at,omitempty"`
	PinSucceededAt    string `json:"pin_succeeded_at,omitempty"`
	ExpireAt          string `json:"expire_at,omitempty"`
	ScheduleUnpinAt   string `json:"schedule_unpin_at,omitempty"`
	UnpinStartAt      string `json:"unpin_start_at,omitempty"`
	UnpinSucceededAt  string `json:"unpin_succeeded_at,omitempty"`
	LastUpdateAt      string `json:"last_update_at"`
	SizeBytes         int64  `json:"size_bytes"`
	SizeHuman         string `json:"size_human"`
	PinAttemptCount   int32  `json:"pin_attempt_count"`
	UnpinAttemptCount int32  `json:"unpin_attempt_count"`
	TTL               string `json:"ttl,omitempty"`
	Age               string `json:"age"`
}

// ToPinRecordView converts a store record to the response view model
func ToPinRecordView(r *store.PinRecord, timeFormat format.TimeFormat) *PinRecordView {
	now := time.Now()
	receivedAt := time.UnixMilli(r.ReceivedAt)

	view := &PinRecordView{
		CID:               r.Cid,
		Status:            format.TranslateStatus(r.Status),
		ReceivedAt:        format.FormatTime(receivedAt, timeFormat),
		LastUpdateAt:      format.FormatTime(time.UnixMilli(r.LastUpdateAt), timeFormat),
		SizeBytes:         r.SizeBytes,
		SizeHuman:         format.FormatBytes(r.SizeBytes),
		PinAttemptCount:   r.PinAttemptCount,
		UnpinAttemptCount: r.UnpinAttemptCount,
		Age:               format.FormatDuration(now.Sub(receivedAt)),
	}

	if r.EnqueuedAt > 0 {
		t := time.UnixMilli(r.EnqueuedAt)
		view.EnqueuedAt = format.FormatTime(t, timeFormat)
	}
	if r.PinStartAt > 0 {
		t := time.UnixMilli(r.PinStartAt)
		view.PinStartAt = format.FormatTime(t, timeFormat)
	}
	if r.PinSucceededAt > 0 {
		t := time.UnixMilli(r.PinSucceededAt)
		view.PinSucceededAt = format.FormatTime(t, timeFormat)
	}
	if r.ExpireAt > 0 {
		t := time.UnixMilli(r.ExpireAt)
		view.ExpireAt = format.FormatTime(t, timeFormat)
		if now.Before(t) {
			view.TTL = format.FormatDuration(t.Sub(now))
		} else {
			view.TTL = "expired"
		}
	}
	if r.ScheduleUnpinAt > 0 {
		t := time.UnixMilli(r.ScheduleUnpinAt)
		view.ScheduleUnpinAt = format.FormatTime(t, timeFormat)
	}
	if r.UnpinStartAt > 0 {
		t := time.UnixMilli(r.UnpinStartAt)
		view.UnpinStartAt = format.FormatTime(t, timeFormat)
	}
	if r.UnpinSucceededAt > 0 {
		t := time.UnixMilli(r.UnpinSucceededAt)
		view.UnpinSucceededAt = format.FormatTime(t, timeFormat)
	}

	return view
}
