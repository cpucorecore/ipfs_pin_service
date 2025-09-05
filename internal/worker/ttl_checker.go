package worker

import (
	"context"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type TTLChecker struct {
	store       store.Store
	queue       mq.Queue
	cfg         *config.Config
	shutdownMgr *shutdown.Manager
}

func NewTTLChecker(store store.Store, queue mq.Queue, cfg *config.Config, shutdownMgr *shutdown.Manager) *TTLChecker {
	return &TTLChecker{store: store, queue: queue, cfg: cfg, shutdownMgr: shutdownMgr}
}

func (c *TTLChecker) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.TTLChecker.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if c.shutdownMgr.IsDraining() {
				log.Log.Sugar().Info("TTL checker stopping due to drain mode")
				return nil
			}
			if err := c.checkTTL(ctx); err != nil {
				log.Log.Sugar().Errorf("checkTTL error: %v", err)
			}
		}
	}
}

func (c *TTLChecker) checkTTL(ctx context.Context) error {
	log.Log.Sugar().Infof("checkTTL start")
	now := time.Now().UnixMilli()
	cids, err := c.store.GetExpireCids(ctx, now, c.cfg.TTLChecker.BatchSize)
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

		log.Log.Sugar().Infof("unpin[%s] with status[%d], expireAt[%d]", cid, pinRecord.Status, pinRecord.ExpireAt)

		err = c.queue.EnqueueUnpin([]byte(cid))
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
			log.Log.Sugar().Errorf("update cid[%s] status[%d]: %v", cid, store.StatusScheduledForUnpin, err)
			continue
		}

		if err = c.store.DeleteExpireIndex(ctx, cid, pinRecord.ExpireAt); err != nil {
			log.Log.Sugar().Errorf("remove expire index for cid[%s] failed: %v", cid, err)
		}
	}
	return nil
}
