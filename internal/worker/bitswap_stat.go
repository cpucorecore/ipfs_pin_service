package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type BitswapStatWorker struct {
	ipfs *ipfs.Client
	cfg  *config.Config
}

func NewBitswapStatWorker(ipfs *ipfs.Client, cfg *config.Config) *BitswapStatWorker {
	return &BitswapStatWorker{ipfs: ipfs, cfg: cfg}
}

func (w *BitswapStatWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.GC.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.run(ctx); err != nil {
				log.Log.Sugar().Errorf("Bitswap stat failed: %v", err)
			}
		}
	}
}

func (w *BitswapStatWorker) run(ctx context.Context) error {
	start := time.Now()
	bs, err := w.ipfs.BitswapStat(ctx)
	monitor.ObserveOperation(monitor.OpBitswapStat, time.Since(start), err)
	if err != nil {
		return err
	}
	monitor.RecordBitswapStat(
		len(bs.Peers), len(bs.Wantlist),
		bs.BlocksReceived, bs.BlocksSent,
		bs.DataReceived, bs.DataSent,
		bs.DupBlksReceived, bs.DupDataReceived,
		bs.MessagesReceived,
	)
	log.Log.Sugar().Infof("Bitswap stat: peers=%d, wantlist=%d, br=%d, bs=%d, dr=%d, ds=%d, dbr=%d, ddr=%d, msgs=%d",
		len(bs.Peers), len(bs.Wantlist),
		bs.BlocksReceived, bs.BlocksSent,
		bs.DataReceived, bs.DataSent,
		bs.DupBlksReceived, bs.DupDataReceived,
		bs.MessagesReceived,
	)
	return nil
}
