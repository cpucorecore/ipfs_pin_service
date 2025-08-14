package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type GCWorker struct {
	ipfs *ipfs.Client
	cfg  *config.Config
}

func NewGCWorker(
	ipfs *ipfs.Client,
	cfg *config.Config,
) *GCWorker {
	return &GCWorker{
		ipfs: ipfs,
		cfg:  cfg,
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
			if err := w.runGC(ctx); err != nil {
				log.Log.Sugar().Errorf("GC failed: %v", err)
			}
		}
	}
}

func (w *GCWorker) runGC(ctx context.Context) error {
	// Execute GC only, no repo/stat calls here
	start := time.Now()
	err := w.ipfs.RepoGC(ctx)
	monitor.ObserveOperation(monitor.OpRepoGC, time.Since(start), err)
	if err != nil {
		return err
	}
	log.Log.Sugar().Infof("GC completed")
	return nil
}
