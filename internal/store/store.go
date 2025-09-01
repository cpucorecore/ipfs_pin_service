package store

import (
	"context"
)

type Store interface {
	Get(ctx context.Context, cid string) (*PinRecord, error)
	Put(ctx context.Context, rec *PinRecord) error
	Update(ctx context.Context, cid string, apply func(*PinRecord) error) error
	AddExpireIndex(ctx context.Context, cid string, expireAt int64) error
	DeleteExpireIndex(ctx context.Context, cid string, expireAt int64) error
	GetExpireCids(ctx context.Context, ts int64, limit int) ([]string, error)
	Close() error
}

type Iterator[T any] interface {
	Next() bool
	Value() T
	Error() error
	Close() error
}
