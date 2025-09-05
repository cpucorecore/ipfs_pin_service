package mq

import (
	"context"
)

const (
	PinQueue     = "pin.queue"
	UnpinQueue   = "unpin.queue"
	ProvideQueue = "provide.queue"

	PinRoutingKey     = "pin"
	UnpinRoutingKey   = "unpin"
	ProvideRoutingKey = "provide"

	PinExchange     = "pin.exchange"
	UnpinExchange   = "unpin.exchange"
	ProvideExchange = "provide.exchange"
)

type MsgHandler func(ctx context.Context, body []byte) error

type Stats struct {
	Messages  int64
	Consumers int64
}

type Queue interface {
	EnqueuePin(data []byte) error
	EnqueueUnpin(data []byte) error
	EnqueueProvide(data []byte) error
	StartPinConsumer(handler MsgHandler)
	StartUnpinConsumer(handler MsgHandler)
	StartProvideConsumer(handler MsgHandler)
	Stats(queue string) (Stats, error)
	Close() error
}
