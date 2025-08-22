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
	cfg       *config.Config
	closeChan chan struct{}
	closeOnce sync.Once

	connMutex      sync.RWMutex
	conn           *amqp.Connection
	ch             *amqp.Channel
	isReconnecting bool
}

func NewRabbitMQ(cfg *config.Config) (*RabbitMQ, error) {
	mq := &RabbitMQ{
		cfg:       cfg,
		closeChan: make(chan struct{}),
	}

	if err := mq.connect(); err != nil {
		return nil, fmt.Errorf("connnect mq: %w", err)
	}

	go mq.monitorConnection()

	return mq, nil
}

func (mq *RabbitMQ) connect() error {
	config := amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	}

	conn, err := amqp.DialConfig(mq.cfg.RabbitMQ.URL, config)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("create channel: %w", err)
	}

	if err = ch.Qos(mq.cfg.RabbitMQ.Prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("set QoS: %w", err)
	}

	if err = mq.setupTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("setup topology: %w", err)
	}

	mq.connMutex.Lock()
	mq.conn = conn
	mq.ch = ch
	mq.connMutex.Unlock()

	return nil
}

func (mq *RabbitMQ) disconnect() {
	mq.connMutex.Lock()
	defer mq.connMutex.Unlock()

	if mq.ch != nil {
		mq.ch.Close()
		mq.ch = nil
	}
	if mq.conn != nil {
		mq.conn.Close()
		mq.conn = nil
	}
}

func (mq *RabbitMQ) reconnect() {
	if !mq.trySetReconnecting() {
		return
	}
	defer mq.setReconnecting(false)

	log.Log.Sugar().Info("Starting RabbitMQ reconnection...")

	backoff := time.Second
	maxBackoff := 30 * time.Second
	maxRetries := 30

	for i := 0; i < maxRetries; i++ {
		select {
		case <-mq.closeChan:
			return
		default:
		}

		mq.disconnect()

		if err := mq.connect(); err != nil {
			log.Log.Sugar().Errorf("Reconnect attempt %d failed: %v", i+1, err)

			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		log.Log.Sugar().Info("RabbitMQ reconnected successfully")
		return
	}

	log.Log.Sugar().Error("Failed to reconnect to RabbitMQ after maximum attempts")
}

func (mq *RabbitMQ) trySetReconnecting() bool {
	mq.connMutex.Lock()
	defer mq.connMutex.Unlock()

	if mq.isReconnecting {
		return false
	}
	mq.isReconnecting = true
	return true
}

func (mq *RabbitMQ) setReconnecting(reconnecting bool) {
	mq.connMutex.Lock()
	defer mq.connMutex.Unlock()
	mq.isReconnecting = reconnecting
}

func (mq *RabbitMQ) getConnectionState() (*amqp.Connection, *amqp.Channel) {
	mq.connMutex.RLock()
	defer mq.connMutex.RUnlock()
	return mq.conn, mq.ch
}

func (mq *RabbitMQ) setupTopology(ch *amqp.Channel) error {
	if err := mq.setupExchangeAndQueue(
		ch,
		mq.cfg.RabbitMQ.Pin.Exchange,
		mq.cfg.RabbitMQ.Pin.Queue,
		mq.cfg.RabbitMQ.Pin.DLX,
		mq.cfg.RabbitMQ.Pin.RetryQueue,
		mq.cfg.RabbitMQ.Pin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup pin queues: %w", err)
	}

	if err := mq.setupExchangeAndQueue(
		ch,
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
	ch *amqp.Channel,
	exchange, queue, dlx, retryQueue string,
	retryDelay time.Duration,
) error {
	if err := mq.declareExchange(ch, exchange, "direct", true); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	if err := mq.declareExchange(ch, dlx, "direct", true); err != nil {
		return fmt.Errorf("declare DLX: %w", err)
	}

	if err := mq.declareQueue(ch, queue, amqp.Table{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": retryQueue,
	}); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	// Declare retry queue
	if err := mq.declareQueue(ch, retryQueue, amqp.Table{
		"x-dead-letter-exchange":    exchange,
		"x-dead-letter-routing-key": queue,
		"x-message-ttl":             retryDelay.Milliseconds(),
	}); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}

	if err := ch.QueueBind(queue, queue, exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	if err := ch.QueueBind(retryQueue, retryQueue, dlx, false, nil); err != nil {
		return fmt.Errorf("bind retry queue: %w", err)
	}

	return nil
}

func (mq *RabbitMQ) declareExchange(ch *amqp.Channel, name, kind string, durable bool) error {
	return ch.ExchangeDeclare(name, kind, durable, false, false, false, nil)
}

func (mq *RabbitMQ) declareQueue(ch *amqp.Channel, name string, args amqp.Table) error {
	_, err := ch.QueueDeclare(name, true, false, false, false, args)
	return err
}

func (mq *RabbitMQ) monitorConnection() {
	for {
		select {
		case <-mq.closeChan:
			return
		default:
		}

		conn, ch := mq.getConnectionState()
		if conn == nil || ch == nil {
			time.Sleep(time.Second)
			continue
		}

		connClosed := conn.NotifyClose(make(chan *amqp.Error))
		channelClosed := ch.NotifyClose(make(chan *amqp.Error))

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
				mq.reconnect()
			}
		}
	}
}

func isChannelNotOpen(err error) bool {
	if err == nil {
		return false
	}

	var amqErr *amqp.Error
	if errors.As(err, &amqErr) {
		return amqErr.Code == 504
	}

	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "channel/connection is not open") || strings.Contains(errMsg, "504")
}

func (mq *RabbitMQ) publishWithChannel(ctx context.Context, ch *amqp.Channel, exchange, key string, body []byte) error {
	return ch.PublishWithContext(ctx,
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
}

func (mq *RabbitMQ) Enqueue(ctx context.Context, exchange string, body []byte) error {
	routingKey := ""
	switch exchange {
	case mq.cfg.RabbitMQ.Pin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Pin.Queue
	case mq.cfg.RabbitMQ.Unpin.Exchange:
		routingKey = mq.cfg.RabbitMQ.Unpin.Queue
	default:
		return fmt.Errorf("unknown exchange: %s", exchange)
	}

	_, ch := mq.getConnectionState()
	if ch == nil {
		return fmt.Errorf("failed to get channel for Enqueue: channel is nil")
	}

	err := mq.publishWithChannel(ctx, ch, exchange, routingKey, body)

	if isChannelNotOpen(err) {
		mq.reconnect()
		_, ch = mq.getConnectionState()
		if ch == nil {
			return fmt.Errorf("failed to get channel after reconnect: channel is nil")
		}

		return mq.publishWithChannel(ctx, ch, exchange, routingKey, body)
	}

	return err
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
			return mq.consumerLoop(ctx, topic, handler)
		})
	}

	return g.Wait()
}

func (mq *RabbitMQ) consumerLoop(ctx context.Context, topic string, handler DeliveryHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-mq.closeChan:
			return fmt.Errorf("queue closed")
		default:
		}

		if err := mq.runConsumer(ctx, topic, handler); err != nil {
			log.Log.Sugar().Warnf("Consumer error: %v, will retry", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-mq.closeChan:
				return fmt.Errorf("queue closed")
			case <-time.After(time.Second):
				continue
			}
		}
	}
}

func (mq *RabbitMQ) runConsumer(ctx context.Context, topic string, handler DeliveryHandler) error {
	conn, _ := mq.getConnectionState()
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	defer ch.Close()

	if err = ch.Qos(mq.cfg.RabbitMQ.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set QoS: %w", err)
	}

	msgs, err := ch.Consume(topic, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consuming: %w", err)
	}

	channelClosed := ch.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-mq.closeChan:
			return fmt.Errorf("queue closed")
		case err = <-channelClosed:
			if err != nil {
				log.Log.Sugar().Warnf("Consumer channel closed: %v", err)
			}
			return fmt.Errorf("channel closed")
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("message channel closed")
			}

			if err = handler(ctx, msg.Body); err != nil {
				msg.Reject(false)
				continue
			}
			msg.Ack(false)
		}
	}
}

func (mq *RabbitMQ) Stats(ctx context.Context, topic string) (Stats, error) {
	_, ch := mq.getConnectionState()
	if ch == nil {
		return Stats{}, fmt.Errorf("failed to get channel for Stats: channel is nil")
	}

	q, err := ch.QueueInspect(topic)
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
		mq.disconnect()
	})
	return nil
}
