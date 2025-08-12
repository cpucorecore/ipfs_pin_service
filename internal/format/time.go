package format

import (
	"fmt"
	"time"
)

// TimeFormat indicates the output time format
type TimeFormat string

const (
	TimeFormatISO   TimeFormat = "iso"
	TimeFormatUnix  TimeFormat = "unix"
	TimeFormatHuman TimeFormat = "human"
)

// FormatTime formats a time using the provided format enum
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

// FormatDuration formats a duration into a short human-readable string
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

// FormatBytes formats bytes as a human-readable string (IEC)
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
