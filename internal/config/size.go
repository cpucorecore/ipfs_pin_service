package config

import (
	"fmt"
	"strconv"
	"strings"
)

// FileSize is a custom type that parses sizes with units
type FileSize int64

// UnmarshalYAML implements yaml.Unmarshaler
func (s *FileSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		// If not a string, try number
		var size int64
		if err := unmarshal(&size); err != nil {
			return err
		}
		*s = FileSize(size)
		return nil
	}

	// Handle special values
	if str == "-1" {
		*s = FileSize(-1)
		return nil
	}

	str = strings.TrimSpace(strings.ToUpper(str))
	if str == "" {
		return fmt.Errorf("empty size string")
	}

	// Extract numeric part
	var multiplier int64 = 1
	var numStr string

	// Parse unit suffix
	switch {
	// Binary prefixes (1024)
	case strings.HasSuffix(str, "KIB"):
		multiplier = 1024
		numStr = str[:len(str)-3]
	case strings.HasSuffix(str, "MIB"):
		multiplier = 1024 * 1024
		numStr = str[:len(str)-3]
	case strings.HasSuffix(str, "GIB"):
		multiplier = 1024 * 1024 * 1024
		numStr = str[:len(str)-3]
	case strings.HasSuffix(str, "TIB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = str[:len(str)-3]

		// Decimal prefixes (1000)
	case strings.HasSuffix(str, "KB"):
		multiplier = 1000
		numStr = str[:len(str)-2]
	case strings.HasSuffix(str, "MB"):
		multiplier = 1000 * 1000
		numStr = str[:len(str)-2]
	case strings.HasSuffix(str, "GB"):
		multiplier = 1000 * 1000 * 1000
		numStr = str[:len(str)-2]
	case strings.HasSuffix(str, "TB"):
		multiplier = 1000 * 1000 * 1000 * 1000
		numStr = str[:len(str)-2]

		// Short forms (treated as decimal)
	case strings.HasSuffix(str, "K"):
		multiplier = 1000
		numStr = str[:len(str)-1]
	case strings.HasSuffix(str, "M"):
		multiplier = 1000 * 1000
		numStr = str[:len(str)-1]
	case strings.HasSuffix(str, "G"):
		multiplier = 1000 * 1000 * 1000
		numStr = str[:len(str)-1]
	case strings.HasSuffix(str, "T"):
		multiplier = 1000 * 1000 * 1000 * 1000
		numStr = str[:len(str)-1]
	default:
		// No unit, assume bytes
		numStr = str
	}

	// Parse number
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return fmt.Errorf("invalid size format: %s", str)
	}

	*s = FileSize(num * float64(multiplier))
	return nil
}

// Int64 returns byte count
func (s FileSize) Int64() int64 {
	return int64(s)
}

// String returns a human-readable size string
func (s FileSize) String() string {
	bytes := int64(s)
	if bytes == -1 {
		return "-1"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	// Use binary prefixes
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGT"[exp])
}
