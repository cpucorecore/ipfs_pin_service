package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type GCWorker struct {
	ipfs        *ipfs.Client
	cfg         *config.Config
	shutdownMgr *shutdown.Manager
}

func NewGCWorker(
	ipfs *ipfs.Client,
	cfg *config.Config,
	shutdownMgr *shutdown.Manager,
) *GCWorker {
	return &GCWorker{
		ipfs:        ipfs,
		cfg:         cfg,
		shutdownMgr: shutdownMgr,
	}
}

func (w *GCWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.GC.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if w.shutdownMgr.IsDraining() {
				log.Log.Sugar().Info("GC worker stopping due to drain mode")
				return nil
			}
			if err := w.runGC(ctx); err != nil {
				log.Log.Sugar().Errorf("GC failed: %v", err)
			}
		}
	}
}

func (w *GCWorker) runGC(ctx context.Context) error {
	start := time.Now()
	err := w.ipfs.RepoGC(ctx)
	monitor.OpDuration.WithLabelValues(monitor.OpRepoGC).Observe(time.Since(start).Seconds())
	log.Log.Sugar().Infof("GC completed, duration: %v", time.Since(start))
	return err
}
