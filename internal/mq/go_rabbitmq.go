package mq

import (
	"context"
	"time"

	"github.com/wagslane/go-rabbitmq"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

type GoRabbitMQ struct {
	cfg       *config.RabbitMQConfig
	amqpCli   *AMQPClient
	conn      *rabbitmq.Conn
	pinPC     *PC
	unpinPC   *PC
	providePC *PC
}

func NewGoRabbitMQ(cfg *config.RabbitMQConfig) (*GoRabbitMQ, error) {
	amqpCli := NewAMQPClient(cfg.URL)

	conn, err := rabbitmq.NewConn(
		cfg.URL,
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
		Concurrency:  cfg.PinConcurrency,
	})

	unpinPC := NewPC(conn, &PCConfig{
		ConsumerName: "unpin",
		Queue:        UnpinQueue,
		RoutingKey:   UnpinRoutingKey,
		Exchange:     UnpinExchange,
		Concurrency:  cfg.UnpinConcurrency,
	})

	providePC := NewPC(conn, &PCConfig{
		ConsumerName: "provide",
		Queue:        ProvideQueue,
		RoutingKey:   ProvideRoutingKey,
		Exchange:     ProvideExchange,
		Concurrency:  cfg.PinConcurrency,
	})

	return &GoRabbitMQ{
		cfg:       cfg,
		amqpCli:   amqpCli,
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

func (g *GoRabbitMQ) Stats(queue string) (int, int, error) {
	return g.amqpCli.QueryQueueStat(queue)
}

func (g *GoRabbitMQ) Close() error {
	g.amqpCli.Close()
	g.pinPC.Close()
	g.unpinPC.Close()
	g.providePC.Close()
	return g.conn.Close()
}
