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
	// reuse TTL checker interval for queue monitoring frequency
	interval := w.cfg.TTLChecker.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
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
		monitor.SetQueueStats(w.cfg.RabbitMQ.Pin.Queue, stats.Ready, stats.Total)
	}
	if stats, err := w.mq.Stats(ctx, w.cfg.RabbitMQ.Unpin.Queue); err == nil {
		monitor.SetQueueStats(w.cfg.RabbitMQ.Unpin.Queue, stats.Ready, stats.Total)
	}
}
