package queue

import (
	"context"
)

// DeliveryHandler 消息处理函数
type DeliveryHandler func(ctx context.Context, body []byte) error

// QueueStats 队列统计
type QueueStats struct {
	Ready   int64 // 准备投递的消息数
	Unacked int64 // 已投递未确认的消息数
	Total   int64 // 总消息数
}

// MessageQueue 定义消息队列接口
type MessageQueue interface {
	// Enqueue 发送消息到指定主题
	Enqueue(ctx context.Context, topic string, body []byte) error

	// Dequeue 消费指定主题的消息
	Dequeue(ctx context.Context, topic string, handler DeliveryHandler) error

	// Stats 获取队列统计信息
	Stats(ctx context.Context, topic string) (QueueStats, error)

	// Close 关闭连接
	Close() error
}
