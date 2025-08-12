package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

type UnpinWorker struct {
	store store.Store
	queue queue.MessageQueue
	ipfs  *ipfs.Client
	cfg   *config.Config
}

func NewUnpinWorker(
	store store.Store,
	queue queue.MessageQueue,
	ipfs *ipfs.Client,
	cfg *config.Config,
) *UnpinWorker {
	return &UnpinWorker{
		store: store,
		queue: queue,
		ipfs:  ipfs,
		cfg:   cfg,
	}
}

func (w *UnpinWorker) Start(ctx context.Context) error {
	return w.queue.Dequeue(ctx, w.cfg.RabbitMQ.Unpin.Queue, w.handleMessage)
}

func (w *UnpinWorker) handleMessage(ctx context.Context, body []byte) error {
	cid := string(body)

	// 更新状态为 Unpinning
	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = int32(store.StatusUnpinning)
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// 执行 unpin
	if err := w.ipfs.PinRm(ctx, cid); err != nil {
		if err.Error() == "pin/rm: not pinned or pinned indirectly" {
			// CID 未被 pin，记录日志并返回成功
			log.Printf("CID %s is already unpinned", cid)
			// 更新状态为 UnpinSucceeded
			err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
				r.Status = int32(store.StatusUnpinSucceeded)
				r.UnpinSucceededAt = time.Now().UnixMilli()
				return nil
			})
			if err != nil {
				log.Printf("Failed to update record status: %v", err)
				return err
			}

			// 删除过期索引
			if err := w.store.DeleteExpireIndex(ctx, cid); err != nil {
				log.Printf("Failed to delete expire index for %s: %v", cid, err)
				// 不返回错误，因为主要操作已经成功
			}
			return nil
		}
		return w.handleUnpinError(ctx, cid, err)
	}

	// 更新状态为 UnpinSucceeded
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = int32(store.StatusUnpinSucceeded)
		r.UnpinSucceededAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// 删除过期索引
	if err := w.store.DeleteExpireIndex(ctx, cid); err != nil {
		log.Printf("Failed to delete expire index for %s: %v", cid, err)
		// 不返回错误，因为主要操作已经成功
	}

	return nil
}

func (w *UnpinWorker) handleUnpinError(ctx context.Context, cid string, err error) error {
	log.Printf("Unpin operation failed for %s: %v", cid, err)

	return w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.UnpinAttemptCount++

		if r.UnpinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = int32(store.StatusDeadLetter)
			return nil
		}

		// 重试
		r.Status = int32(store.StatusScheduledForUnpin)
		return nil
	})
}
