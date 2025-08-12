package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	cfg       *config.Config
	closeOnce sync.Once
	closeChan chan struct{}
}

func NewRabbitMQ(cfg *config.Config) (*RabbitMQ, error) {
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create channel: %w", err)
	}

	if err := ch.Qos(cfg.RabbitMQ.Prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("set QoS: %w", err)
	}

	mq := &RabbitMQ{
		conn:      conn,
		ch:        ch,
		cfg:       cfg,
		closeChan: make(chan struct{}),
	}

	if err := mq.setupTopology(); err != nil {
		mq.Close()
		return nil, fmt.Errorf("setup topology: %w", err)
	}

	return mq, nil
}

func (mq *RabbitMQ) setupTopology() error {
	// Pin 相关队列
	if err := mq.setupExchangeAndQueue(
		mq.cfg.RabbitMQ.Pin.Exchange,
		mq.cfg.RabbitMQ.Pin.Queue,
		mq.cfg.RabbitMQ.Pin.DLX,
		mq.cfg.RabbitMQ.Pin.RetryQueue,
		mq.cfg.RabbitMQ.Pin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup pin queues: %w", err)
	}

	// Unpin 相关队列
	if err := mq.setupExchangeAndQueue(
		mq.cfg.RabbitMQ.Unpin.Exchange,
		mq.cfg.RabbitMQ.Unpin.Queue,
		mq.cfg.RabbitMQ.Unpin.DLX,
		mq.cfg.RabbitMQ.Unpin.RetryQueue,
		mq.cfg.RabbitMQ.Unpin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup unpin queues: %w", err)
	}

	return nil
}

func (mq *RabbitMQ) setupExchangeAndQueue(
	exchange, queue, dlx, retryQueue string,
	retryDelay time.Duration,
) error {
	// 声明主交换机
	if err := mq.ch.ExchangeDeclare(
		exchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	// 声明死信交换机
	if err := mq.ch.ExchangeDeclare(
		dlx,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare DLX: %w", err)
	}

	// 声明主队列
	if _, err := mq.ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": dlx,
		},
	); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	// 声明重试队列
	if _, err := mq.ch.QueueDeclare(
		retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": exchange,
			"x-message-ttl":          int64(retryDelay.Milliseconds()),
		},
	); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}

	// 绑定队列
	if err := mq.ch.QueueBind(
		queue,
		queue,
		exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	if err := mq.ch.QueueBind(
		retryQueue,
		retryQueue,
		dlx,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind retry queue: %w", err)
	}

	return nil
}

func (mq *RabbitMQ) Enqueue(ctx context.Context, exchange string, body []byte) error {
	// 使用队列名作为 routing key
	routingKey := ""
	switch exchange {
	case mq.cfg.RabbitMQ.Pin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Pin.Queue
	case mq.cfg.RabbitMQ.Unpin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Unpin.Queue
	default:
		return fmt.Errorf("unknown exchange: %s", exchange)
	}

	return mq.ch.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key (queue name)
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
}

func (mq *RabbitMQ) Dequeue(ctx context.Context, topic string, handler DeliveryHandler) error {
	msgs, err := mq.ch.Consume(
		topic, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("start consuming: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-mq.closeChan:
			return fmt.Errorf("queue closed")
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("channel closed")
			}

			err := handler(ctx, msg.Body)
			if err != nil {
				// 拒绝消息，让它进入死信队列
				msg.Reject(false)
				continue
			}

			msg.Ack(false)
		}
	}
}

func (mq *RabbitMQ) Stats(ctx context.Context, topic string) (Stats, error) {
	q, err := mq.ch.QueueInspect(topic)
	if err != nil {
		return Stats{}, fmt.Errorf("inspect queue: %w", err)
	}

	return Stats{
		Ready:   int64(q.Messages),
		Unacked: int64(q.Consumers),
		Total:   int64(q.Messages + q.Consumers),
	}, nil
}

func (mq *RabbitMQ) Close() error {
	mq.closeOnce.Do(func() {
		close(mq.closeChan)
		if mq.ch != nil {
			mq.ch.Close()
		}
		if mq.conn != nil {
			mq.conn.Close()
		}
	})
	return nil
}
