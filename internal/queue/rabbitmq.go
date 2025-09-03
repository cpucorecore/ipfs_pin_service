package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	connectionManager ConnectionManager
	mu                sync.RWMutex
	channel           *amqp.Channel
	queueConfig       map[string]config.QueueConf
	shutdownMgr       *shutdown.Manager
}

func NewRabbitMQ(cfg *config.Config, shutdownMgr *shutdown.Manager) (*RabbitMQ, error) {
	connectionManager := NewConnectionManager(cfg.RabbitMQ.URL)
	channel, err := connectionManager.CreateChannel()
	if err != nil {
		return nil, err
	}

	queueConfig := make(map[string]config.QueueConf)
	queueConfig[cfg.RabbitMQ.Pin.Exchange] = cfg.RabbitMQ.Pin
	queueConfig[cfg.RabbitMQ.Unpin.Exchange] = cfg.RabbitMQ.Unpin
	queueConfig[cfg.RabbitMQ.Provide.Exchange] = cfg.RabbitMQ.Provide

	rabbitmq := &RabbitMQ{
		connectionManager: connectionManager,
		channel:           channel,
		queueConfig:       queueConfig,
		shutdownMgr:       shutdownMgr,
	}

	if err = rabbitmq.setupAllQueues(channel, cfg); err != nil {
		channel.Close()
		connectionManager.Close()
		return nil, fmt.Errorf("setup queues: %w", err)
	}

	return rabbitmq, nil
}

func (mq *RabbitMQ) setupAllQueues(channel *amqp.Channel, cfg *config.Config) error {
	if err := mq.setupExchangeAndQueue(channel, &cfg.RabbitMQ.Pin); err != nil {
		return fmt.Errorf("setup pin queues: %w", err)
	}

	if err := mq.setupExchangeAndQueue(channel, &cfg.RabbitMQ.Unpin); err != nil {
		return fmt.Errorf("setup unpin queues: %w", err)
	}

	if err := mq.setupExchangeAndQueue(channel, &cfg.RabbitMQ.Provide); err != nil {
		return fmt.Errorf("setup provide queues: %w", err)
	}

	return nil
}

func (mq *RabbitMQ) setupExchangeAndQueue(channel *amqp.Channel, queueConf *config.QueueConf) error {
	if err := channel.ExchangeDeclare(
		queueConf.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", queueConf.Exchange, err)
	}

	if err := channel.ExchangeDeclare(
		queueConf.DLX,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare DLX %s: %w", queueConf.DLX, err)
	}

	if _, err := channel.QueueDeclare(
		queueConf.Queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    queueConf.DLX,
			"x-dead-letter-routing-key": queueConf.RetryQueue,
		},
	); err != nil {
		return fmt.Errorf("declare queue %s: %w", queueConf.Queue, err)
	}

	if _, err := channel.QueueDeclare(
		queueConf.RetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    queueConf.Exchange,
			"x-dead-letter-routing-key": queueConf.Queue,
			"x-message-ttl":             queueConf.RetryDelay.Milliseconds(),
		},
	); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", queueConf.RetryQueue, err)
	}

	if err := channel.QueueBind(
		queueConf.Queue,
		queueConf.Queue,
		queueConf.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind queue %s: %w", queueConf.Queue, err)
	}

	if err := channel.QueueBind(
		queueConf.RetryQueue,
		queueConf.RetryQueue,
		queueConf.DLX,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind retry queue %s: %w", queueConf.RetryQueue, err)
	}

	return nil
}

func (mq *RabbitMQ) getChannel() *amqp.Channel {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return mq.channel
}

func (mq *RabbitMQ) setChannel(channel *amqp.Channel) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.channel = channel
}

func (mq *RabbitMQ) closeChannel() {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if mq.channel != nil {
		mq.channel.Close()
		mq.channel = nil
	}
}

var (
	ErrCreateChannel = errors.New("create channel error")
)

func (mq *RabbitMQ) mustRecreateChannel() error {
	retry := 0
	const maxRetries = 30
	retryDelay := time.Second
	const maxRetryDelay = time.Second * 30

	for retry < maxRetries {
		channel, err := mq.connectionManager.CreateChannel()
		if err == nil {
			mq.closeChannel()
			mq.setChannel(channel)
			return nil
		}

		log.Log.Sugar().Errorf("Failed to create channel retry=%d err=[%s]", retry, err)
		time.Sleep(retryDelay)
		retryDelay *= 2
		if retryDelay > maxRetryDelay {
			retryDelay = maxRetryDelay
		}
		retry++
	}

	log.Log.Sugar().Errorf("failed to create channel retry=%d, no more request will enqueue", retry)
	return ErrCreateChannel
}

func (mq *RabbitMQ) mustPublish(ctx context.Context, exchange, key string, body []byte) error {
	msg := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}

	var err error
	retry := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = mq.getChannel().PublishWithContext(ctx, exchange, key, false, false, msg)
		if err == nil {
			break
		} else {
			retry++
			log.Log.Sugar().Errorf("Failed to publish message to queue %s, body=[%s], err=[%s], channle IsClosed=[%v], retry=%d", key, string(body), err.Error(), mq.getChannel().IsClosed(), retry)
			if mq.getChannel().IsClosed() {
				log.Log.Sugar().Warnf("channel is closed, try to recreate it")
				err = mq.mustRecreateChannel()
				if err != nil {
					log.Log.Sugar().Warnf("1recreate channel fail, connection stat:%v", mq.connectionManager.GetConnection().IsClosed())
					return err
				}
			} else {
				log.Log.Sugar().Warnf("channel is not closed, TODO check it")
			}

			if retry > 3 {
				retry = 0
				err = mq.mustRecreateChannel()
				if err != nil {
					log.Log.Sugar().Warnf("2recreate channel fail, connection stat:%v", mq.connectionManager.GetConnection().IsClosed())
					return err
				}
			}
			time.Sleep(time.Second)
		}
	}

	return nil
}

var (
	ErrWrongExchange = errors.New("wrong exchange")
)

func (mq *RabbitMQ) getRoutingKey(exchange string) (string, error) {
	c, ok := mq.queueConfig[exchange]
	if !ok {
		return "", ErrWrongExchange
	}
	return c.Queue, nil
}

func (mq *RabbitMQ) Enqueue(ctx context.Context, exchange string, body []byte) error {
	routingKey, err := mq.getRoutingKey(exchange)
	if err != nil {
		return err
	}

	return mq.mustPublish(ctx, exchange, routingKey, body)
}

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
		default:
		}

		if err := mq.runConsumer(ctx, topic, handler); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
	}
}

var (
	ErrChannelClosed    = errors.New("channel is closed")
	ErrMsgChannelClosed = errors.New("message channel is closed")
)

func (mq *RabbitMQ) runConsumer(ctx context.Context, topic string, handler DeliveryHandler) error {
	ch, err := mq.connectionManager.CreateChannel()
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	defer ch.Close()

	if err = ch.Qos(1, 0, false); err != nil {
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

		case <-mq.shutdownMgr.ShutdownCtx().Done():
			log.Log.Sugar().Info("Consumer received shutdown signal, exiting")
			return mq.shutdownMgr.ShutdownCtx().Err()

		case err = <-channelClosed:
			log.Log.Sugar().Warnf("channel closed with no error")
			if err != nil {
				log.Log.Sugar().Warnf("channel closed with error: %v", err)
			}
			return ErrChannelClosed

		case msg, ok := <-msgs:
			if !ok {
				return ErrMsgChannelClosed
			}

			if err = handler(ctx, msg.Body); err != nil {
				msg.Reject(false)
				continue
			}
			msg.Ack(false)
		}
	}
}

var (
	DefaultStats = Stats{}
)

func (mq *RabbitMQ) Stats(ctx context.Context, topic string) (Stats, error) {
	mq.mu.RLock()
	defer mq.mu.RUnlock()

	if mq.channel == nil {
		return DefaultStats, ErrChannelClosed
	}

	q, err := mq.channel.QueueInspect(topic)
	if err != nil {
		return Stats{}, fmt.Errorf("inspect queue: %w", err)
	}

	return Stats{
		Messages:  int64(q.Messages),
		Consumers: int64(q.Consumers),
	}, nil
}

func (mq *RabbitMQ) Close() error {
	mq.closeChannel()
	mq.connectionManager.Close()
	return nil
}
