package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type QueueConf struct {
	Exchange   string        `yaml:"exchange"`
	Queue      string        `yaml:"queue"`
	DLX        string        `yaml:"dlx"`
	RetryQueue string        `yaml:"retry_queue"`
	RetryDelay time.Duration `yaml:"retry_delay"`
}

type RabbitMQConf struct {
	URL      string    `yaml:"url"`
	Prefetch int       `yaml:"prefetch"`
	Pin      QueueConf `yaml:"pin"`
	Unpin    QueueConf `yaml:"unpin"`
	Provide  QueueConf `yaml:"provide"`
}

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

	RabbitMQ RabbitMQConf `yaml:"rabbitmq"`

	Workers struct {
		PinConcurrency     int           `yaml:"pin_concurrency"`
		UnpinConcurrency   int           `yaml:"unpin_concurrency"`
		ProvideConcurrency int           `yaml:"provide_concurrency"`
		ProvideRecursive   bool          `yaml:"provide_recursive"`
		MaxRetries         int           `yaml:"max_retries"`
		PinTimeout         time.Duration `yaml:"pin_timeout"`
		UnpinTimeout       time.Duration `yaml:"unpin_timeout"`
		ProvideTimeout     time.Duration `yaml:"provide_timeout"`
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

func Load(name string) (*Config, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
