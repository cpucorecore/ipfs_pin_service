package queue

import (
	"context"
)

type DeliveryHandler func(ctx context.Context, body []byte) error

type Stats struct {
	Ready   int64
	Unacked int64
	Total   int64
}

type MessageQueue interface {
	Enqueue(ctx context.Context, topic string, body []byte) error
	Dequeue(ctx context.Context, topic string, handler DeliveryHandler) error
	DequeueConcurrent(ctx context.Context, topic string, concurrency int, handler DeliveryHandler) error
	Stats(ctx context.Context, topic string) (Stats, error)
	Close() error
}
