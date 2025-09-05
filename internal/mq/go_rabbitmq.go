package mq

import (
	"context"
	"time"

	"github.com/wagslane/go-rabbitmq"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

type GoRabbitMQ struct {
	config    *config.Config
	conn      *rabbitmq.Conn
	pinPC     *PC
	unpinPC   *PC
	providePC *PC
}

func NewGoRabbitMQ(cfg *config.Config) (*GoRabbitMQ, error) {
	conn, err := rabbitmq.NewConn(
		cfg.RabbitMQ.URL,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(2*time.Second),
	)
	if err != nil {
		return nil, err
	}

	pinPC := NewPC(conn, &PCConfig{
		ConsumerName: "pin",
		Queue:        PinQueue,
		RoutingKey:   PinRoutingKey,
		Exchange:     PinExchange,
		Concurrency:  cfg.Workers.PinConcurrency,
	})

	unpinPC := NewPC(conn, &PCConfig{
		ConsumerName: "unpin",
		Queue:        UnpinQueue,
		RoutingKey:   UnpinRoutingKey,
		Exchange:     UnpinExchange,
		Concurrency:  cfg.Workers.UnpinConcurrency,
	})

	providePC := NewPC(conn, &PCConfig{
		ConsumerName: "provide",
		Queue:        ProvideQueue,
		RoutingKey:   ProvideRoutingKey,
		Exchange:     ProvideExchange,
		Concurrency:  cfg.Workers.ProvideConcurrency,
	})

	return &GoRabbitMQ{
		config:    cfg,
		conn:      conn,
		pinPC:     pinPC,
		unpinPC:   unpinPC,
		providePC: providePC,
	}, nil
}

func (g *GoRabbitMQ) EnqueuePin(data []byte) error {
	return g.pinPC.Enqueue(data)
}

func (g *GoRabbitMQ) EnqueueUnpin(data []byte) error {
	return g.unpinPC.Enqueue(data)
}

func (g *GoRabbitMQ) EnqueueProvide(data []byte) error {
	return g.providePC.Enqueue(data)
}

func (g *GoRabbitMQ) StartPinConsumer(handler MsgHandler) {
	g.pinPC.StartConsume(context.Background(), handler)
}

func (g *GoRabbitMQ) StartUnpinConsumer(handler MsgHandler) {
	g.unpinPC.StartConsume(context.Background(), handler)
}

func (g *GoRabbitMQ) StartProvideConsumer(handler MsgHandler) {
	g.providePC.StartConsume(context.Background(), handler)
}

func (g *GoRabbitMQ) Stats(queue string) (Stats, error) {
	return Stats{}, nil
}

func (g *GoRabbitMQ) Close() error {
	g.pinPC.Close()
	g.unpinPC.Close()
	g.providePC.Close()
	return g.conn.Close()
}
