package store

import (
	"context"

	"github.com/cpucorecore/ipfs_pin_service/internal/model"
)

// Store 定义存储接口
type Store interface {
	// Get 获取记录
	Get(ctx context.Context, cid string) (*model.PinRecord, error)

	// Put 写入记录
	Put(ctx context.Context, rec *model.PinRecord) error

	// Update 更新记录
	Update(ctx context.Context, cid string, apply func(*model.PinRecord) error) error

	// IndexByStatus 按状态查询
	IndexByStatus(ctx context.Context, s model.Status) (Iterator[string], error)

	// IndexByExpireBefore 按过期时间查询
	IndexByExpireBefore(ctx context.Context, ts int64, limit int) ([]string, error)

	// Close 关闭存储
	Close() error
}

// Iterator 定义迭代器接口
type Iterator[T any] interface {
	Next() bool
	Value() T
	Error() error
	Close() error
}
