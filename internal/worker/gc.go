package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/model"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"google.golang.org/protobuf/proto"
)

type GCWorker struct {
	store store.Store
	queue queue.MessageQueue
	ipfs  *ipfs.Client
	cfg   *config.Config
}

func NewGCWorker(
	store store.Store,
	queue queue.MessageQueue,
	ipfs *ipfs.Client,
	cfg *config.Config,
) *GCWorker {
	return &GCWorker{
		store: store,
		queue: queue,
		ipfs:  ipfs,
		cfg:   cfg,
	}
}

func (w *GCWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.GC.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.runGC(ctx); err != nil {
				log.Printf("GC failed: %v", err)
			}
		}
	}
}

func (w *GCWorker) runGC(ctx context.Context) error {
	// 检查过期的记录
	now := time.Now().UnixMilli()
	expiredRecords, err := w.store.IndexByExpireBefore(ctx, now, 100)
	if err != nil {
		log.Printf("Failed to get expired records: %v", err)
		return err
	}

	// 将过期记录加入 unpin 队列
	for _, cid := range expiredRecords {
		rec, err := w.store.Get(ctx, cid)
		if err != nil {
			log.Printf("Failed to get record %s: %v", cid, err)
			continue
		}
		if rec == nil {
			continue
		}

		// 更新状态为 ScheduledForUnpin
		err = w.store.Update(ctx, cid, func(r *model.PinRecord) error {
			r.Status = int32(model.StatusScheduledForUnpin)
			r.ScheduleUnpinAt = now
			return nil
		})
		if err != nil {
			log.Printf("Failed to update record %s: %v", cid, err)
			continue
		}

		// 入队
		body, err := proto.Marshal(rec)
		if err != nil {
			log.Printf("Failed to marshal record %s: %v", cid, err)
			continue
		}

		if err := w.queue.Enqueue(ctx, w.cfg.RabbitMQ.Unpin.Exchange, body); err != nil {
			log.Printf("Failed to enqueue record %s: %v", cid, err)
			continue
		}

		log.Printf("Scheduled record %s for unpin", cid)
	}

	// 获取 GC 前的状态
	beforeStat, err := w.ipfs.RepoStat(ctx)
	if err != nil {
		return err
	}

	// 执行 GC
	report, err := w.ipfs.RepoGC(ctx)
	if err != nil {
		return err
	}

	// 获取 GC 后的状态
	afterStat, err := w.ipfs.RepoStat(ctx)
	if err != nil {
		return err
	}

	log.Printf("GC completed: removed %d keys, freed %d bytes, processed %d expired records",
		report.KeysRemoved,
		beforeStat.RepoSize-afterStat.RepoSize,
		len(expiredRecords))

	return nil
}
