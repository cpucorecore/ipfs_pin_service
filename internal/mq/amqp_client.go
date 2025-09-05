package mq

import (
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type AMQPClient struct {
	url     string
	conn    *amqp091.Connection
	channel *amqp091.Channel
}

func NewAMQPClient(url string) *AMQPClient {
	c := &AMQPClient{
		url: url,
	}
	err := c.Connect()
	if err != nil {
		panic(err)
	}
	return c
}

func (c *AMQPClient) Connect() error {
	conn, err := amqp091.Dial(c.url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	c.conn = conn
	c.channel = channel
	return nil
}

func (c *AMQPClient) Close() {
	if c.channel != nil {
		c.channel.Close()
		c.channel = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *AMQPClient) ReConnect() error {
	c.Close()
	return c.Connect()
}

func (c *AMQPClient) QueryQueueStat(queue string) (int, int, error) {
	q, err := c.channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		c.ReConnect()
	}

	q, err = c.channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, 0, err
	}

	return q.Messages, q.Consumers, nil
}
