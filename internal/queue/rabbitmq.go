package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	connectionManager ConnectionManager
	channel           *amqp.Channel
	queueConfig       map[string]config.QueueConf
}

func NewRabbitMQ(cfg *config.Config) (*RabbitMQ, error) {
	connectionManager := NewConnectionManager(cfg.RabbitMQ.URL)
	channel, err := connectionManager.CreateChannel()
	if err != nil {
		return nil, err
	}

	queueConfig := make(map[string]config.QueueConf)
	queueConfig[cfg.RabbitMQ.Pin.Exchange] = cfg.RabbitMQ.Pin
	queueConfig[cfg.RabbitMQ.Unpin.Exchange] = cfg.RabbitMQ.Unpin

	return &RabbitMQ{
		connectionManager: connectionManager,
		channel:           channel,
		queueConfig:       queueConfig,
	}, nil
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

func (mq *RabbitMQ) publishWithChannel(ctx context.Context, exchange, key string, body []byte) error {
	return mq.channel.PublishWithContext(ctx,
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

	return mq.publishWithChannel(ctx, exchange, routingKey, body)
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

func (mq *RabbitMQ) Stats(ctx context.Context, topic string) (Stats, error) {
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
	mq.channel.Close()
	mq.connectionManager.Close()
	return nil
}
