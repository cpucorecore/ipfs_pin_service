package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn           *amqp.Connection
	ch             *amqp.Channel
	cfg            *config.Config
	closeOnce      sync.Once
	closeChan      chan struct{}
	reconnectMutex sync.Mutex
	isReconnecting bool
}

func NewRabbitMQ(cfg *config.Config) (*RabbitMQ, error) {
	config := amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	}

	conn, err := amqp.DialConfig(cfg.RabbitMQ.URL, config)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create channel: %w", err)
	}

	if err = ch.Qos(cfg.RabbitMQ.Prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("set QoS: %w", err)
	}

	mq := &RabbitMQ{
		conn:           conn,
		ch:             ch,
		cfg:            cfg,
		closeChan:      make(chan struct{}),
		isReconnecting: false,
	}

	if err = mq.setupTopology(); err != nil {
		mq.Close()
		return nil, fmt.Errorf("setup topology: %w", err)
	}

	go mq.monitorConnection()
	return mq, nil
}

func (mq *RabbitMQ) setupTopology() error {
	if err := mq.setupExchangeAndQueue(
		mq.cfg.RabbitMQ.Pin.Exchange,
		mq.cfg.RabbitMQ.Pin.Queue,
		mq.cfg.RabbitMQ.Pin.DLX,
		mq.cfg.RabbitMQ.Pin.RetryQueue,
		mq.cfg.RabbitMQ.Pin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup pin queues: %w", err)
	}

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
	// Declare primary exchange (with recovery)
	if err := mq.declareExchangeWithRecovery(exchange, "direct", true); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	// Declare DLX (with recovery)
	if err := mq.declareExchangeWithRecovery(dlx, "direct", true); err != nil {
		return fmt.Errorf("declare DLX: %w", err)
	}

	// Declare primary queue (with recovery)
	if err := mq.declareQueueWithRecovery(queue, amqp.Table{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": retryQueue, // route errors to retry queue via DLX
	}); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	// Declare retry queue (with recovery)
	if err := mq.declareQueueWithRecovery(retryQueue, amqp.Table{
		"x-dead-letter-exchange":    exchange, // after TTL, route back to primary exchange
		"x-dead-letter-routing-key": queue,    // using primary queue as routing key
		"x-message-ttl":             int64(retryDelay.Milliseconds()),
	}); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}

	// Bind queues
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

// declareExchangeWithRecovery tries to declare an exchange and, on PRECONDITION_FAILED,
// deletes and re-creates it with the new parameters.
func (mq *RabbitMQ) declareExchangeWithRecovery(name, kind string, durable bool) error {
	err := mq.ch.ExchangeDeclare(
		name,
		kind,
		durable,
		false,
		false,
		false,
		nil,
	)
	if err == nil {
		return nil
	}

	if isPreconditionFailed(err) {
		// Channel is closed by broker on 406; reopen and then recreate
		if rerr := mq.reopenChannel(); rerr != nil {
			return rerr
		}
		_ = mq.ch.ExchangeDelete(name, false, false)
		return mq.ch.ExchangeDeclare(name, kind, durable, false, false, false, nil)
	}
	if isChannelNotOpen(err) {
		if rerr := mq.reopenChannel(); rerr != nil {
			return rerr
		}
		return mq.ch.ExchangeDeclare(name, kind, durable, false, false, false, nil)
	}
	return err
}

// declareQueueWithRecovery tries to declare a queue and, on PRECONDITION_FAILED,
// deletes and re-creates it so parameter changes (like TTL) take effect.
func (mq *RabbitMQ) declareQueueWithRecovery(name string, args amqp.Table) error {
	_, err := mq.ch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		args,
	)
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		// Channel is closed by broker on 406; reopen and then recreate
		if rerr := mq.reopenChannel(); rerr != nil {
			return rerr
		}
		_, _ = mq.ch.QueueDelete(name, false, false, false)
		_, err = mq.ch.QueueDeclare(name, true, false, false, false, args)
		return err
	}
	if isChannelNotOpen(err) {
		if rerr := mq.reopenChannel(); rerr != nil {
			return rerr
		}
		_, err = mq.ch.QueueDeclare(name, true, false, false, false, args)
		return err
	}
	return err
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}

	var amqErr *amqp.Error
	if errors.As(err, &amqErr) {
		return amqErr.Code == 406
	}
	return strings.Contains(strings.ToUpper(err.Error()), "PRECONDITION_FAILED") || strings.Contains(err.Error(), "406")
}

func isChannelNotOpen(err error) bool {
	if err == nil {
		return false
	}

	var amqErr *amqp.Error
	if errors.As(err, &amqErr) {
		return amqErr.Code == 504
	}

	s := strings.ToLower(err.Error())
	return strings.Contains(s, "channel/connection is not open") || strings.Contains(s, "504")
}

func (mq *RabbitMQ) reopenChannel() error {
	if mq.conn == nil {
		return fmt.Errorf("amqp connection is nil")
	}

	ch, err := mq.conn.Channel()
	if err != nil {
		return fmt.Errorf("reopen channel: %w", err)
	}

	if err = ch.Qos(mq.cfg.RabbitMQ.Prefetch, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("set QoS on reopened channel: %w", err)
	}

	if mq.ch != nil {
		_ = mq.ch.Close()
	}
	mq.ch = ch
	return nil
}

func (mq *RabbitMQ) Enqueue(ctx context.Context, exchange string, body []byte) error {
	// Route using queue name
	routingKey := ""
	switch exchange {
	case mq.cfg.RabbitMQ.Pin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Pin.Queue
	case mq.cfg.RabbitMQ.Unpin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Unpin.Queue
	default:
		return fmt.Errorf("unknown exchange: %s", exchange)
	}

	err := mq.ch.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key (queue name)
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain", // 使用文本类型，提高可读性
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)

	// 如果通道关闭，尝试重新打开
	if isChannelNotOpen(err) {
		if rerr := mq.reopenChannel(); rerr != nil {
			return fmt.Errorf("failed to reopen channel: %w", rerr)
		}
		// 重试发布
		return mq.ch.PublishWithContext(ctx,
			exchange,   // exchange
			routingKey, // routing key (queue name)
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType:  "text/plain", // 使用文本类型，提高可读性
				Body:         body,
				DeliveryMode: amqp.Persistent,
			},
		)
	}

	return err
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

			err = handler(ctx, msg.Body)
			if err != nil {
				// Reject to route into DLX
				msg.Reject(false)
				continue
			}

			msg.Ack(false)
		}
	}
}

// DequeueConcurrent starts `concurrency` independent consumers on the same queue.
// Each consumer uses its own channel (AMQP channels are not goroutine-safe).
func (mq *RabbitMQ) DequeueConcurrent(ctx context.Context, topic string, concurrency int, handler DeliveryHandler) error {
	if concurrency <= 0 {
		concurrency = 1
	}

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			ch, err := mq.conn.Channel()
			if err != nil {
				return fmt.Errorf("create channel: %w", err)
			}
			defer ch.Close()

			if err := ch.Qos(mq.cfg.RabbitMQ.Prefetch, 0, false); err != nil {
				return fmt.Errorf("set QoS: %w", err)
			}

			msgs, err := ch.Consume(
				topic,
				"",
				false,
				false,
				false,
				false,
				nil,
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
					if err := handler(ctx, msg.Body); err != nil {
						msg.Reject(false)
						continue
					}
					msg.Ack(false)
				}
			}
		})
	}

	return g.Wait()
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

// monitorConnection 监控连接状态并在断开时自动重连
func (mq *RabbitMQ) monitorConnection() {
	connClosed := mq.conn.NotifyClose(make(chan *amqp.Error))
	channelClosed := mq.ch.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case <-mq.closeChan:
			return
		case err := <-connClosed:
			if err != nil {
				log.Log.Sugar().Warnf("RabbitMQ connection closed: %v", err)
				mq.reconnect()
			}
		case err := <-channelClosed:
			if err != nil {
				log.Log.Sugar().Warnf("RabbitMQ channel closed: %v", err)
				mq.reopenChannel()
			}
		}
	}
}

// reconnect 重新连接到 RabbitMQ
func (mq *RabbitMQ) reconnect() {
	mq.reconnectMutex.Lock()
	defer mq.reconnectMutex.Unlock()

	if mq.isReconnecting {
		return
	}

	mq.isReconnecting = true
	defer func() { mq.isReconnecting = false }()

	log.Log.Sugar().Info("Starting RabbitMQ reconnection...")

	// 指数退避重连
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for i := 0; i < 10; i++ { // 最多重试 10 次
		select {
		case <-mq.closeChan:
			return
		default:
		}

		// 配置连接参数
		config := amqp.Config{
			Heartbeat: 30 * time.Second,
			Locale:    "en_US",
		}

		conn, err := amqp.DialConfig(mq.cfg.RabbitMQ.URL, config)
		if err != nil {
			log.Log.Sugar().Errorf("Reconnect attempt %d failed: %v", i+1, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			log.Log.Sugar().Errorf("Create channel failed during reconnect: %v", err)
			time.Sleep(backoff)
			continue
		}

		if err := ch.Qos(mq.cfg.RabbitMQ.Prefetch, 0, false); err != nil {
			ch.Close()
			conn.Close()
			log.Log.Sugar().Errorf("Set QoS failed during reconnect: %v", err)
			time.Sleep(backoff)
			continue
		}

		// 关闭旧连接
		if mq.conn != nil {
			mq.conn.Close()
		}
		if mq.ch != nil {
			mq.ch.Close()
		}

		// 更新连接
		mq.conn = conn
		mq.ch = ch

		// 重新设置拓扑
		if err := mq.setupTopology(); err != nil {
			log.Log.Sugar().Errorf("Setup topology failed during reconnect: %v", err)
			time.Sleep(backoff)
			continue
		}

		log.Log.Sugar().Info("RabbitMQ reconnected successfully")

		// 重新启动监控
		go mq.monitorConnection()
		return
	}

	log.Log.Sugar().Error("Failed to reconnect to RabbitMQ after 10 attempts")
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
