package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
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
	return w.queue.DequeueConcurrent(ctx, w.cfg.RabbitMQ.Unpin.Queue, w.cfg.Workers.UnpinConcurrency, w.handleMessage)
}

func (w *UnpinWorker) handleMessage(ctx context.Context, body []byte) error {
	cid := string(body)

	if !util.CheckCid(cid) {
		log.Printf("Warning: wrong unpin cid: %s", cid)
		return nil
	}

	// Mark as Unpinning
	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinning
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// Execute unpin
	if err = w.ipfs.PinRm(ctx, cid); err != nil {
		if err.Error() == "pin/rm: not pinned or pinned indirectly" {
			// Not pinned already; treat as success
			log.Printf("CID %s is already unpinned", cid)
			// Mark as UnpinSucceeded
			err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
				r.Status = store.StatusUnpinSucceeded
				r.UnpinSucceededAt = time.Now().UnixMilli()
				return nil
			})
			if err != nil {
				log.Printf("Failed to update record status: %v", err)
				return err
			}

			// Delete expire index entry
			if err := w.store.DeleteExpireIndex(ctx, cid); err != nil {
				log.Printf("Failed to delete expire index for %s: %v", cid, err)
				// Log only; main action succeeded
			}
			return nil
		}
		return w.handleUnpinError(ctx, cid, err)
	}

	// Mark as UnpinSucceeded
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinSucceeded
		r.UnpinSucceededAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// Delete expire index entry
	if err = w.store.DeleteExpireIndex(ctx, cid); err != nil {
		log.Printf("Failed to delete expire index for %s: %v", cid, err)
		// Log only; main action succeeded
	}

	return nil
}

func (w *UnpinWorker) handleUnpinError(ctx context.Context, cid string, err error) error {
	log.Printf("Unpin operation failed for %s: %v", cid, err)

	return w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.UnpinAttemptCount++

		if r.UnpinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
			return nil
		}

		// Retry
		r.Status = store.StatusScheduledForUnpin
		return nil
	})
}
