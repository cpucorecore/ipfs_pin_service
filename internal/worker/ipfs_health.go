package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
)

type IPFSHealthWorker struct {
	ipfs *ipfs.Client
	cfg  *config.Config
}

func NewIPFSHealthWorker(ipfs *ipfs.Client, cfg *config.Config) *IPFSHealthWorker {
	return &IPFSHealthWorker{ipfs: ipfs, cfg: cfg}
}

func (w *IPFSHealthWorker) Start(ctx context.Context) error {
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
			// Use RepoStat as a lightweight health check
			if _, err := w.ipfs.RepoStat(ctx); err == nil {
				monitor.SetIPFSAvailable(true)
			} else {
				monitor.SetIPFSAvailable(false)
			}
		}
	}
}
