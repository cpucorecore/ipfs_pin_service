package config

import (
	"fmt"
	"strconv"
	"strings"
)

// FileSize 是一个自定义类型，用于解析带单位的文件大小
type FileSize int64

// UnmarshalYAML 实现 yaml.Unmarshaler 接口
func (s *FileSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		// 如果不是字符串，尝试解析为数字
		var size int64
		if err := unmarshal(&size); err != nil {
			return err
		}
		*s = FileSize(size)
		return nil
	}

	// 处理特殊值
	if str == "-1" {
		*s = FileSize(-1)
		return nil
	}

	str = strings.TrimSpace(strings.ToUpper(str))
	if str == "" {
		return fmt.Errorf("empty size string")
	}

	// 解析数字部分
	var multiplier int64 = 1
	var numStr string

	// 查找单位
	switch {
	// 二进制前缀 (1024)
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

	// 十进制前缀 (1000)
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

	// 简写形式 (按十进制处理)
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
		// 没有单位，假设是字节
		numStr = str
	}

	// 解析数字
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return fmt.Errorf("invalid size format: %s", str)
	}

	*s = FileSize(num * float64(multiplier))
	return nil
}

// Int64 返回字节数
func (s FileSize) Int64() int64 {
	return int64(s)
}

// String 返回人类可读的大小字符串
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

	// 使用二进制前缀
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGT"[exp])
}
