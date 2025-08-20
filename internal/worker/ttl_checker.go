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
	log.Log.Sugar().Infof("TTL check start")
	now := time.Now().UnixMilli()
	cids, err := c.store.IndexByExpireBefore(ctx, now, c.cfg.TTLChecker.BatchSize)
	if err != nil {
		return err
	}
	log.Log.Sugar().Infof("TTL check end with %d cids", len(cids))

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

		// 直接发送 CID 字符串，简单高效
		body := []byte(cid)

		// 重试机制：最多重试3次
		var enqueueErr error
		for retry := 0; retry < 3; retry++ {
			enqueueErr = c.queue.Enqueue(ctx, "unpin.exchange", body)
			if enqueueErr == nil {
				break
			}

			// 如果是连接错误，等待一段时间后重试
			if retry < 2 {
				log.Log.Sugar().Warnf("queue.Enqueue(%s) retry %d/3 err: %v", cid, retry+1, enqueueErr)
				time.Sleep(time.Second * time.Duration(retry+1)) // 指数退避：1s, 2s
			}
		}

		if enqueueErr != nil {
			log.Log.Sugar().Errorf("queue.Enqueue(%s) failed after 3 retries: %v", cid, enqueueErr)
			continue
		}
	}
	return nil
}
