package mq

import (
	"context"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"
)

type PC struct {
	cfg       *PCConfig
	publisher *rabbitmq.Publisher
	consumer  *rabbitmq.Consumer
}

func (p *PC) Enqueue(data []byte) error {
	return p.publisher.Publish(
		data,
		[]string{p.cfg.RoutingKey},
		rabbitmq.WithPublishOptionsContentType("text/plain"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(p.cfg.Exchange))
}

func (p *PC) StartConsume(ctx context.Context, handler MsgHandler) {
	go func() {
		err := p.consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			err := handler(ctx, d.Body)
			if err != nil {
				return rabbitmq.NackRequeue
			}
			return rabbitmq.Ack
		})
		if err != nil {
			log.Log.Sugar().Error("consumer[%s] run error", p.cfg.ConsumerName, zap.Error(err))
		}
	}()
}

func (p *PC) Close() {
	p.publisher.Close()
	p.consumer.Close()
}

func NewPC(conn *rabbitmq.Conn, cfg *PCConfig) *PC {
	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(cfg.Exchange),
		rabbitmq.WithPublisherOptionsExchangeKind("direct"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		panic(err)
	}

	consumer, err := rabbitmq.NewConsumer(
		conn,
		cfg.Queue,
		rabbitmq.WithConsumerOptionsConcurrency(cfg.Concurrency),
		rabbitmq.WithConsumerOptionsQOSPrefetch(cfg.Concurrency),
		rabbitmq.WithConsumerOptionsConsumerName(cfg.ConsumerName),
		rabbitmq.WithConsumerOptionsRoutingKey(cfg.RoutingKey),
		rabbitmq.WithConsumerOptionsExchangeName(cfg.Exchange),
		rabbitmq.WithConsumerOptionsExchangeKind("direct"),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)

	pc := &PC{
		cfg:       cfg,
		publisher: publisher,
		consumer:  consumer,
	}
	return pc
}
