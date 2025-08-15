package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log struct {
		Level         string        `yaml:"level"`
		Async         bool          `yaml:"async"`
		BufferSize    FileSize      `yaml:"buffer_size"`
		FlushInterval time.Duration `yaml:"flush_interval"`
	} `yaml:"log"`
	HTTP struct {
		Port int `yaml:"port"`
	} `yaml:"http"`
	IPFS struct {
		APIAddr     string        `yaml:"api_addr"`
		HTTPTimeout time.Duration `yaml:"http_timeout"`
		DialTimeout time.Duration `yaml:"dial_timeout"`
	} `yaml:"ipfs"`

	RabbitMQ struct {
		URL      string `yaml:"url"`
		Prefetch int    `yaml:"prefetch"`
		Pin      struct {
			Exchange   string        `yaml:"exchange"`
			Queue      string        `yaml:"queue"`
			DLX        string        `yaml:"dlx"`
			RetryQueue string        `yaml:"retry_queue"`
			RetryDelay time.Duration `yaml:"retry_delay"`
		} `yaml:"pin"`
		Unpin struct {
			Exchange   string        `yaml:"exchange"`
			Queue      string        `yaml:"queue"`
			DLX        string        `yaml:"dlx"`
			RetryQueue string        `yaml:"retry_queue"`
			RetryDelay time.Duration `yaml:"retry_delay"`
		} `yaml:"unpin"`
	} `yaml:"rabbitmq"`

	Workers struct {
		PinConcurrency   int           `yaml:"pin_concurrency"`
		UnpinConcurrency int           `yaml:"unpin_concurrency"`
		MaxRetries       int           `yaml:"max_retries"`
		PinTimeout       time.Duration `yaml:"pin_timeout"`
		UnpinTimeout     time.Duration `yaml:"unpin_timeout"`
	} `yaml:"workers"`

	GC struct {
		Interval time.Duration `yaml:"interval"`
	} `yaml:"gc"`

	TTLChecker struct {
		Interval  time.Duration `yaml:"interval"`
		BatchSize int           `yaml:"batch_size"`
	} `yaml:"ttl_checker"`

	TTL struct {
		Default time.Duration `yaml:"default"`
		Table   []struct {
			MaxSize FileSize      `yaml:"max_size"`
			TTL     time.Duration `yaml:"ttl"`
		} `yaml:"table"`
	} `yaml:"ttl"`

	Filter struct {
		SizeLimit FileSize `yaml:"size_limit"`
	} `yaml:"filter"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
