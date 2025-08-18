package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type StatWorker struct {
	ipfs *ipfs.Client
	cfg  *config.Config
}

func NewStatWorker(ipfs *ipfs.Client, cfg *config.Config) *StatWorker {
	return &StatWorker{ipfs: ipfs, cfg: cfg}
}

func (w *StatWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.runStat(ctx); err != nil {
				log.Log.Sugar().Errorf("Repo stat failed: %v", err)
			}
		}
	}
}

func (w *StatWorker) runStat(ctx context.Context) error {
	start := time.Now()
	stat, err := w.ipfs.RepoStat(ctx)
	monitor.OpDuration.WithLabelValues(monitor.OpRepoStat).Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	monitor.RecordRepoStat(stat.RepoSize, stat.StorageMax, stat.NumObjects, stat.RepoPath, stat.Version)
	log.Log.Sugar().Infof("Repo stat: size=%d, max=%d, objects=%d, path=%s, version=%s, api duration=%f seconds",
		stat.RepoSize, stat.StorageMax, stat.NumObjects, stat.RepoPath, stat.Version, time.Since(start).Seconds())

	return nil
}
