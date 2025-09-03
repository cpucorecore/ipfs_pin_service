package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type QueueMonitor struct {
	mq          queue.MessageQueue
	cfg         *config.Config
	shutdownMgr *shutdown.Manager
}

func NewQueueMonitor(mq queue.MessageQueue, cfg *config.Config, shutdownMgr *shutdown.Manager) *QueueMonitor {
	return &QueueMonitor{mq: mq, cfg: cfg, shutdownMgr: shutdownMgr}
}

func (w *QueueMonitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if w.shutdownMgr != nil && w.shutdownMgr.IsDraining() {
				log.Log.Sugar().Info("Queue monitor stopping due to drain mode")
				return nil
			}
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
