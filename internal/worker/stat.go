package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
)

type StatWorker struct {
	ipfs *ipfs.Client
	cfg  *config.Config
}

func NewStatWorker(ipfs *ipfs.Client, cfg *config.Config) *StatWorker {
	return &StatWorker{ipfs: ipfs, cfg: cfg}
}

func (w *StatWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.GC.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.runStat(ctx); err != nil {
				log.Printf("Repo stat failed: %v", err)
			}
		}
	}
}

func (w *StatWorker) runStat(ctx context.Context) error {
	start := time.Now()
	stat, err := w.ipfs.RepoStat(ctx)
	monitor.ObserveOperation(monitor.OpRepoStat, time.Since(start), err)
	if err != nil {
		return err
	}
	monitor.RecordRepoStat(stat.RepoSize, stat.StorageMax, stat.NumObjects, stat.RepoPath, stat.Version)
	log.Printf("Repo stat: size=%d, max=%d, objects=%d, path=%s, version=%s",
		stat.RepoSize, stat.StorageMax, stat.NumObjects, stat.RepoPath, stat.Version)

	// Queue stats (use MessageQueue Stats via a small helper worker if needed). Here we skip direct call to avoid circular deps.

	return nil
}
