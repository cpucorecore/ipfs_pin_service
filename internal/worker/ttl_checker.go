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
			if err := c.checkTTL(ctx); err != nil {
				log.Log.Sugar().Errorf("checkTTL error: %v", err)
			}
		}
	}
}

func (c *TTLChecker) checkTTL(ctx context.Context) error {
	log.Log.Sugar().Infof("checkTTL start")
	now := time.Now().UnixMilli()
	cids, err := c.store.IndexByExpireBefore(ctx, now, c.cfg.TTLChecker.BatchSize)
	if err != nil {
		return err
	}
	log.Log.Sugar().Infof("checkTTL success with %d cids", len(cids))

	if len(cids) == 0 {
		return nil
	}

	return c.publishUnpinCids(ctx, cids)
}

func (c *TTLChecker) publishUnpinCids(ctx context.Context, cids []string) error {
	log.Log.Sugar().Debugf("publishUnpinCids start with %d cids", len(cids))
	for _, cid := range cids {
		pinRecord, err := c.store.Get(ctx, cid)
		if err != nil {
			log.Log.Sugar().Errorf("store.Get(%s) err: %v", cid, err)
			continue
		}

		if pinRecord == nil {
			log.Log.Sugar().Warnf("store.Get(%s) nil", cid)
			continue
		}

		if pinRecord.Status != store.StatusActive && pinRecord.Status != store.StatusDeadLetter {
			log.Log.Sugar().Warnf("unpin[%s] with wrong status[%d]", cid, pinRecord.Status)
			continue
		}

		err = c.queue.Enqueue(ctx, "unpin.exchange", []byte(cid))
		if err != nil {
			log.Log.Sugar().Errorf("queue.Enqueue(%s) err: %v", cid, err)
			continue
		}

		err = c.store.Update(ctx, cid, func(r *store.PinRecord) error {
			r.Status = store.StatusScheduledForUnpin
			r.ScheduleUnpinAt = time.Now().UnixMilli()
			return nil
		})
		if err != nil {
			log.Log.Sugar().Errorf("store.Update(%s) err: %v", cid, err)
		}
	}
	return nil
}
