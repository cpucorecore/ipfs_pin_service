package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type TTLChecker struct {
	store store.Store
	queue queue.MessageQueue
	cfg   *config.Config
}

func NewTTLChecker(store store.Store, queue queue.MessageQueue, cfg *config.Config) *TTLChecker {
	return &TTLChecker{store: store, queue: queue, cfg: cfg}
}

func (c *TTLChecker) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.TTLChecker.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.check(ctx); err != nil {
				log.Log.Sugar().Errorf("TTL check error: %v", err)
			}
		}
	}
}

func (c *TTLChecker) check(ctx context.Context) error {
	now := time.Now().UnixMilli()
	cids, err := c.store.IndexByExpireBefore(ctx, now, c.cfg.TTLChecker.BatchSize)
	if err != nil {
		return err
	}

	for _, cid := range cids {
		rec, err := c.store.Get(ctx, cid)
		if err != nil {
			log.Log.Sugar().Errorf("store.Get(%s) err: %v", cid, err)
			continue
		}
		if rec == nil {
			continue
		}

		if err = c.store.Update(ctx, cid, func(r *store.PinRecord) error {
			r.Status = store.StatusScheduledForUnpin
			r.ScheduleUnpinAt = time.Now().UnixMilli()
			return nil
		}); err != nil {
			log.Log.Sugar().Errorf("store.Update(%s) err: %v", cid, err)
			continue
		}

		err = c.queue.Enqueue(ctx, "unpin.exchange", []byte(cid))
		if err != nil {
			log.Log.Sugar().Errorf("queue.Enqueue(%s) err: %v", cid, err)
			continue
		}
	}
	return nil
}
