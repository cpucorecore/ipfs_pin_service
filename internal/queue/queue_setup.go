package queue

import (
	"fmt"
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Setup struct {
	Pin   *config.QueueConf
	Unpin *config.QueueConf
}

func NewSetup(pin *config.QueueConf, unpin *config.QueueConf) *Setup {
	return &Setup{
		Pin:   pin,
		Unpin: unpin,
	}
}

func (c *Setup) setupQueues(channel *amqp.Channel) error {
	if err := c.setupExchangeAndQueue(
		channel,
		c.Pin.Exchange,
		c.Pin.Queue,
		c.Pin.DLX,
		c.Pin.RetryQueue,
		c.Pin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup pin queues: %w", err)
	}

	if err := c.setupExchangeAndQueue(
		channel,
		c.Unpin.Exchange,
		c.Unpin.Queue,
		c.Unpin.DLX,
		c.Unpin.RetryQueue,
		c.Unpin.RetryDelay,
	); err != nil {
		return fmt.Errorf("setup unpin queues: %w", err)
	}

	return nil
}

func (c *Setup) setupExchangeAndQueue(
	ch *amqp.Channel,
	exchange, queue, dlx, retryQueue string,
	retryDelay time.Duration,
) error {
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare DLX: %w", err)
	}

	if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": retryQueue,
	}); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
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
