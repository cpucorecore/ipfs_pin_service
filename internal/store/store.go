package store

import (
	"context"
)

type Expire struct {
	Key []byte
	Cid string
}

type Store interface {
	Get(ctx context.Context, cid string) (*PinRecord, error)
	Put(ctx context.Context, rec *PinRecord) error
	Update(ctx context.Context, cid string, apply func(*PinRecord) error) error
	AddExpireIndex(ctx context.Context, cid string, expireAt int64) error
	DeleteExpireIndex(ctx context.Context, cid string, expireAt int64) error
	DeleteExpireIndexByKey(ctx context.Context, key []byte) error
	GetExpires(ctx context.Context, ts int64, limit int) ([]*Expire, error)
	Close() error
}

type Iterator[T any] interface {
	Next() bool
	Value() T
	Error() error
	Close() error
}
