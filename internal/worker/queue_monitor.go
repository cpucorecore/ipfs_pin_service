package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
)

type QueueMonitor struct {
	mq  queue.MessageQueue
	cfg *config.Config
}

func NewQueueMonitor(mq queue.MessageQueue, cfg *config.Config) *QueueMonitor {
	return &QueueMonitor{mq: mq, cfg: cfg}
}

func (w *QueueMonitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.sample(ctx)
		}
	}
}

func (w *QueueMonitor) sample(ctx context.Context) {
	if stats, err := w.mq.Stats(ctx, w.cfg.RabbitMQ.Pin.Queue); err == nil {
		monitor.SetQueueStats(w.cfg.RabbitMQ.Pin.Queue, stats.Messages, stats.Consumers)
	}
	if stats, err := w.mq.Stats(ctx, w.cfg.RabbitMQ.Unpin.Queue); err == nil {
		monitor.SetQueueStats(w.cfg.RabbitMQ.Unpin.Queue, stats.Messages, stats.Consumers)
	}
	if stats, err := w.mq.Stats(ctx, w.cfg.RabbitMQ.Provide.Queue); err == nil {
		monitor.SetQueueStats(w.cfg.RabbitMQ.Provide.Queue, stats.Messages, stats.Consumers)
	}
}
