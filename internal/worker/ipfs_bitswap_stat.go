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
	ticker := time.NewTicker(time.Second * 10)
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
	if err != nil {
		return err
	}
	monitor.OpDuration.WithLabelValues(monitor.OpBitswapStat).Observe(time.Since(start).Seconds())
	wantList := bs.GetWantlist()
	monitor.RecordBitswapStat(
		len(bs.Peers), len(wantList),
		bs.BlocksReceived, bs.BlocksSent,
		bs.DataReceived, bs.DataSent,
		bs.DupBlksReceived, bs.DupDataReceived,
		bs.MessagesReceived,
	)
	return nil
}
