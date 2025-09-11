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
	cfg         *config.TTLCheckerConfig
	store       store.Store
	queue       mq.Queue
	shutdownMgr *shutdown.Manager
}

func NewTTLChecker(cfg *config.TTLCheckerConfig, store store.Store, queue mq.Queue, shutdownMgr *shutdown.Manager) *TTLChecker {
	return &TTLChecker{
		cfg:         cfg,
		store:       store,
		queue:       queue,
		shutdownMgr: shutdownMgr,
	}
}

func (c *TTLChecker) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Interval)
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
	expires, err := c.store.GetExpires(ctx, now, c.cfg.BatchSize)
	if err != nil {
		return err
	}
	log.Log.Sugar().Infof("checkTTL success with %d expires", len(expires))

	if len(expires) == 0 {
		return nil
	}

	return c.publishUnpinCids(ctx, expires)
}

func (c *TTLChecker) publishUnpinCids(ctx context.Context, expires []*store.Expire) error {
	log.Log.Sugar().Debugf("publishUnpinCids start with %d cids", len(expires))
	for _, expire := range expires {
		pinRecord, err := c.store.Get(ctx, expire.Cid)
		if err != nil {
			log.Log.Sugar().Errorf("store.Get(%s) err: %v", expire.Cid, err)
			continue
		}

		if pinRecord == nil {
			log.Log.Sugar().Warnf("store.Get(%s) nil", expire.Cid)
			continue
		}

		log.Log.Sugar().Infof("unpin[%s] with status[%d], expireAt[%d]", expire.Cid, pinRecord.Status, pinRecord.ExpireAt)

		err = c.queue.EnqueueUnpin([]byte(expire.Cid))
		if err != nil {
			log.Log.Sugar().Errorf("queue.Enqueue(%s) err: %v", expire.Cid, err)
			continue
		}

		err = c.store.Update(ctx, expire.Cid, func(r *store.PinRecord) error {
			r.Status = store.StatusScheduledForUnpin
			r.ScheduleUnpinAt = time.Now().UnixMilli()
			return nil
		})
		if err != nil {
			log.Log.Sugar().Errorf("update cid[%s] status[%d]: %v", expire.Cid, store.StatusScheduledForUnpin, err)
			continue
		}

		if err = c.store.DeleteExpireIndexByKey(ctx, expire.Key); err != nil {
			log.Log.Sugar().Errorf("remove expire index for cid[%s] failed: %v", expire.Cid, err)
		}
	}
	return nil
}
