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

type StatWorker struct {
	ipfs        *ipfs.Client
	cfg         *config.Config
	shutdownMgr *shutdown.Manager
}

func NewStatWorker(ipfs *ipfs.Client, cfg *config.Config, shutdownMgr *shutdown.Manager) *StatWorker {
	return &StatWorker{ipfs: ipfs, cfg: cfg, shutdownMgr: shutdownMgr}
}

func (w *StatWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if w.shutdownMgr.IsDraining() {
				log.Log.Sugar().Info("Stat worker stopping due to drain mode")
				return nil
			}
			if err := w.runStat(ctx); err != nil {
				log.Log.Sugar().Errorf("Repo stat failed: %v", err)
			}
		}
	}
}

func (w *StatWorker) runStat(ctx context.Context) error {
	start := time.Now()
	stat, err := w.ipfs.RepoStat(ctx)
	if err != nil {
		monitor.SetIPFSAvailable(false)
		return err
	}

	monitor.SetIPFSAvailable(true)
	monitor.OpDuration.WithLabelValues(monitor.OpRepoStat).Observe(time.Since(start).Seconds())
	monitor.RecordRepoStat(stat.RepoSize, stat.StorageMax, stat.NumObjects, stat.RepoPath, stat.Version)

	return nil
}
