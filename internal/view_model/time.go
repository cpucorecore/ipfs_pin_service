package view_model

import "time"

type TimeFormat string

const (
	TimeFormatISO   TimeFormat = "iso"
	TimeFormatUnix  TimeFormat = "unix"
	TimeFormatHuman TimeFormat = "human"
)

func FormatTime(t time.Time, format TimeFormat) string {
	switch format {
	case TimeFormatUnix:
		return t.Format("2006-01-02 15:04:05")
	case TimeFormatHuman:
		return t.Format("2006-01-02 15:04:05 MST")
	default:
		return t.Format(time.RFC3339)
	}
}
