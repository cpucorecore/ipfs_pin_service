package queue

import (
	"testing"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/stretchr/testify/assert"
)

func TestRabbitMQ_QueueConfig(t *testing.T) {
	cfg := &config.Config{
		RabbitMQ: config.RabbitMQConf{
			URL: "amqp://guest:guest@localhost:5672/",
			Pin: config.QueueConf{
				Exchange:   "test.pin.exchange",
				Queue:      "test.pin.queue",
				DLX:        "test.pin.dlx",
				RetryQueue: "test.pin.retry.queue",
			},
			Unpin: config.QueueConf{
				Exchange:   "test.unpin.exchange",
				Queue:      "test.unpin.queue",
				DLX:        "test.unpin.dlx",
				RetryQueue: "test.unpin.retry.queue",
			},
			Provide: config.QueueConf{
				Exchange:   "test.provide.exchange",
				Queue:      "test.provide.queue",
				DLX:        "test.provide.dlx",
				RetryQueue: "test.provide.retry.queue",
			},
		},
	}

	// 注意：这个测试可能需要运行中的 RabbitMQ 实例
	// 在实际测试环境中，可能需要跳过或者使用 mock
	t.Skip("Skipping RabbitMQ integration test - requires running RabbitMQ instance")

	shutdownMgr := shutdown.NewManager()
	rabbitmq, err := NewRabbitMQ(cfg, shutdownMgr)
	if err != nil {
		t.Fatalf("Failed to create RabbitMQ: %v", err)
	}
	defer rabbitmq.Close()

	// 验证队列配置是否正确设置
	assert.Equal(t, cfg.RabbitMQ.Pin.Queue, rabbitmq.queueConfig[cfg.RabbitMQ.Pin.Exchange].Queue)
	assert.Equal(t, cfg.RabbitMQ.Unpin.Queue, rabbitmq.queueConfig[cfg.RabbitMQ.Unpin.Exchange].Queue)
	assert.Equal(t, cfg.RabbitMQ.Provide.Queue, rabbitmq.queueConfig[cfg.RabbitMQ.Provide.Exchange].Queue)

	// 验证所有三个队列配置都存在
	assert.Len(t, rabbitmq.queueConfig, 3)
	assert.Contains(t, rabbitmq.queueConfig, cfg.RabbitMQ.Pin.Exchange)
	assert.Contains(t, rabbitmq.queueConfig, cfg.RabbitMQ.Unpin.Exchange)
	assert.Contains(t, rabbitmq.queueConfig, cfg.RabbitMQ.Provide.Exchange)
}

func TestRabbitMQ_GetRoutingKey(t *testing.T) {
	cfg := &config.Config{
		RabbitMQ: config.RabbitMQConf{
			Pin: config.QueueConf{
				Exchange: "test.pin.exchange",
				Queue:    "test.pin.queue",
			},
			Unpin: config.QueueConf{
				Exchange: "test.unpin.exchange",
				Queue:    "test.unpin.queue",
			},
			Provide: config.QueueConf{
				Exchange: "test.provide.exchange",
				Queue:    "test.provide.queue",
			},
		},
	}

	rabbitmq := &RabbitMQ{
		queueConfig: map[string]config.QueueConf{
			cfg.RabbitMQ.Pin.Exchange:     cfg.RabbitMQ.Pin,
			cfg.RabbitMQ.Unpin.Exchange:   cfg.RabbitMQ.Unpin,
			cfg.RabbitMQ.Provide.Exchange: cfg.RabbitMQ.Provide,
		},
	}

	// 测试正确的 exchange 名称
	pinQueue, err := rabbitmq.getRoutingKey(cfg.RabbitMQ.Pin.Exchange)
	assert.NoError(t, err)
	assert.Equal(t, cfg.RabbitMQ.Pin.Queue, pinQueue)

	unpinQueue, err := rabbitmq.getRoutingKey(cfg.RabbitMQ.Unpin.Exchange)
	assert.NoError(t, err)
	assert.Equal(t, cfg.RabbitMQ.Unpin.Queue, unpinQueue)

	provideQueue, err := rabbitmq.getRoutingKey(cfg.RabbitMQ.Provide.Exchange)
	assert.NoError(t, err)
	assert.Equal(t, cfg.RabbitMQ.Provide.Queue, provideQueue)

	// 测试错误的 exchange 名称（使用队列名称而不是交换机名称）
	_, err = rabbitmq.getRoutingKey(cfg.RabbitMQ.Pin.Queue) // 这会导致 "wrong exchange" 错误
	assert.Error(t, err)
	assert.Equal(t, ErrWrongExchange, err)
}
