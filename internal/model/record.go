package model

import (
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/cpucorecore/ipfs_pin_service/proto"
)

// formatBytes 将字节数转换为人类可读的格式
func formatBytes(bytes int64) string {
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

// formatDuration 将持续时间转换为人类可读的格式
func formatDuration(d time.Duration) string {
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

// PinRecord 是对 protobuf 生成的 PinRecord 的包装
type PinRecord struct {
	*pb.PinRecord
}

// PinRecordView 是用于 API 响应的 JSON 视图
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
	TTL               string `json:"ttl,omitempty"` // 剩余 TTL
	Age               string `json:"age"`           // 从创建到现在的时间
}

// ToView 将 protobuf 记录转换为 JSON 视图
// TimeStyle 定义时间格式化的风格
type TimeStyle string

const (
	TimeStyleISO   TimeStyle = "iso"   // ISO 8601 格式
	TimeStyleUnix  TimeStyle = "unix"  // Unix 时间戳
	TimeStyleHuman TimeStyle = "human" // 人类可读格式
)

func formatTimeByStyle(t time.Time, style TimeStyle) string {
	switch style {
	case TimeStyleUnix:
		return fmt.Sprintf("%d", t.Unix())
	case TimeStyleHuman:
		return t.Format("2006-01-02 15:04:05 MST")
	default: // TimeStyleISO
		return t.Format(time.RFC3339)
	}
}

func (r *PinRecord) ToView(timeStyle TimeStyle) *PinRecordView {
	now := time.Now()
	receivedAt := time.UnixMilli(r.ReceivedAt)

	view := &PinRecordView{
		CID:               r.Cid,
		Status:            Status(r.Status).String(),
		ReceivedAt:        formatTimeByStyle(receivedAt, timeStyle),
		LastUpdateAt:      formatTimeByStyle(time.UnixMilli(r.LastUpdateAt), timeStyle),
		SizeBytes:         r.SizeBytes,
		SizeHuman:         formatBytes(r.SizeBytes),
		PinAttemptCount:   r.PinAttemptCount,
		UnpinAttemptCount: r.UnpinAttemptCount,
		Age:               formatDuration(now.Sub(receivedAt)),
	}

	// 设置所有时间字段
	if r.EnqueuedAt > 0 {
		t := time.UnixMilli(r.EnqueuedAt)
		view.EnqueuedAt = formatTimeByStyle(t, timeStyle)
	}
	if r.PinStartAt > 0 {
		t := time.UnixMilli(r.PinStartAt)
		view.PinStartAt = formatTimeByStyle(t, timeStyle)
	}
	if r.PinSucceededAt > 0 {
		t := time.UnixMilli(r.PinSucceededAt)
		view.PinSucceededAt = formatTimeByStyle(t, timeStyle)
	}
	if r.ExpireAt > 0 {
		t := time.UnixMilli(r.ExpireAt)
		view.ExpireAt = formatTimeByStyle(t, timeStyle)
		// 计算剩余 TTL
		if now.Before(t) {
			view.TTL = formatDuration(t.Sub(now))
		} else {
			view.TTL = "expired"
		}
	}
	if r.ScheduleUnpinAt > 0 {
		t := time.UnixMilli(r.ScheduleUnpinAt)
		view.ScheduleUnpinAt = formatTimeByStyle(t, timeStyle)
	}
	if r.UnpinStartAt > 0 {
		t := time.UnixMilli(r.UnpinStartAt)
		view.UnpinStartAt = formatTimeByStyle(t, timeStyle)
	}
	if r.UnpinSucceededAt > 0 {
		t := time.UnixMilli(r.UnpinSucceededAt)
		view.UnpinSucceededAt = formatTimeByStyle(t, timeStyle)
	}

	return view
}

// MarshalJSON 实现自定义 JSON 序列化
func (r *PinRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.ToView(TimeStyleISO))
}

// NewPinRecord 创建新的 pin 记录
func NewPinRecord(cid string) *PinRecord {
	now := time.Now().UnixMilli()
	return &PinRecord{
		PinRecord: &pb.PinRecord{
			Cid:          cid,
			Status:       int32(StatusReceived),
			ReceivedAt:   now,
			LastUpdateAt: now,
		},
	}
}
