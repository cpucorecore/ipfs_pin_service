package format

import (
	"fmt"
	"time"
)

// TimeFormat 定义时间格式类型
type TimeFormat string

const (
	TimeFormatISO   TimeFormat = "iso"
	TimeFormatUnix  TimeFormat = "unix"
	TimeFormatHuman TimeFormat = "human"
)

// FormatTime 根据指定格式格式化时间
func FormatTime(t time.Time, format TimeFormat) string {
	switch format {
	case TimeFormatUnix:
		return fmt.Sprintf("%d", t.Unix())
	case TimeFormatHuman:
		return t.Format("2006-01-02 15:04:05 MST")
	default:
		return t.Format(time.RFC3339)
	}
}

// FormatDuration 格式化时间间隔为人类可读格式
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

// FormatBytes 格式化字节大小为人类可读格式
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
