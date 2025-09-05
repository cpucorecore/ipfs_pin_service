package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type QueueMonitor struct {
	mq          mq.Queue
	cfg         *config.Config
	shutdownMgr *shutdown.Manager
}

func NewQueueMonitor(mq mq.Queue, cfg *config.Config, shutdownMgr *shutdown.Manager) *QueueMonitor {
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
			w.sample()
		}
	}
}

func (w *QueueMonitor) sample() {
	var messages, consumers int
	var err error

	if messages, consumers, err = w.mq.Stats(mq.PinQueue); err == nil {
		monitor.SetQueueStats(mq.PinQueue, messages, consumers)
	}

	if messages, consumers, err = w.mq.Stats(mq.UnpinQueue); err == nil {
		monitor.SetQueueStats(mq.UnpinQueue, messages, consumers)
	}
	if messages, consumers, err = w.mq.Stats(mq.ProvideQueue); err == nil {
		monitor.SetQueueStats(mq.ProvideQueue, messages, consumers)
	}
}
