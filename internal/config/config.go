package config

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Level         string        `yaml:"level"`
	Async         bool          `yaml:"async"`
	BufferSize    FileSize      `yaml:"buffer_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
}

type IPFSConfig struct {
	APIAddr     string        `yaml:"api_addr"`
	HTTPTimeout time.Duration `yaml:"http_timeout"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

type RabbitMQConfig struct {
	URL                string `yaml:"url"`
	PinConcurrency     int    `yaml:"pin_concurrency"`
	UnpinConcurrency   int    `yaml:"unpin_concurrency"`
	ProvideConcurrency int    `yaml:"provide_concurrency"`
}

type WorkerConfig struct {
	Timeout       time.Duration `yaml:"timeout"`
	MaxRetry      int           `yaml:"max_retry"`
	RetryInterval time.Duration `yaml:"retry_interval"`
}

type GCConfig struct {
	Interval time.Duration `yaml:"interval"`
}

type TTLCheckerConfig struct {
	Interval  time.Duration `yaml:"interval"`
	BatchSize int           `yaml:"batch_size"`
}

type TTLConfig struct {
	Default time.Duration `yaml:"default"`
	Table   []struct {
		MaxSize FileSize      `yaml:"max_size"`
		TTL     time.Duration `yaml:"ttl"`
	} `yaml:"table"`
}

type FilterConfig struct {
	SizeLimit FileSize `yaml:"size_limit"`
}

type Config struct {
	Log              *LogConfig        `yaml:"log"`
	HTTP             *HTTPConfig       `yaml:"http"`
	IPFS             *IPFSConfig       `yaml:"ipfs"`
	RabbitMQ         *RabbitMQConfig   `yaml:"rabbitmq"`
	GC               *GCConfig         `yaml:"gc"`
	TTLChecker       *TTLCheckerConfig `yaml:"ttl_checker"`
	TTL              *TTLConfig        `yaml:"ttl"`
	Filter           *FilterConfig     `yaml:"filter"`
	ProvideRecursive bool              `yaml:"provide_recursive"`
	PinWorker        *WorkerConfig     `yaml:"pin_worker"`
	UnpinWorker      *WorkerConfig     `yaml:"unpin_worker"`
	ProvideWorker    *WorkerConfig     `yaml:"provide_worker"`
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

	data, _ = json.MarshalIndent(cfg, "", "  ")
	log.Printf("Loaded config:\n%s", string(data))

	return cfg, nil
}
