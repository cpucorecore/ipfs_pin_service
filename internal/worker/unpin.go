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
	var rec model.PinRecord
	if err := proto.Unmarshal(body, &rec); err != nil {
		log.Printf("Failed to unmarshal record: %v", err)
		return err
	}

	// 更新状态为 Unpinning
	err := w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.Status = int32(model.StatusUnpinning)
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// 执行 unpin
	if err := w.ipfs.PinRm(ctx, rec.Cid); err != nil {
		return w.handleUnpinError(ctx, &rec, err)
	}

	// 更新状态为 UnpinSucceeded
	err = w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.Status = int32(model.StatusUnpinSucceeded)
		r.UnpinSucceededAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	return nil
}

func (w *UnpinWorker) handleUnpinError(ctx context.Context, rec *model.PinRecord, err error) error {
	log.Printf("Unpin operation failed for %s: %v", rec.Cid, err)

	return w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.UnpinAttemptCount++

		if r.UnpinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = int32(model.StatusDeadLetter)
			return nil
		}

		// 重试
		r.Status = int32(model.StatusScheduledForUnpin)
		return nil
	})
}
