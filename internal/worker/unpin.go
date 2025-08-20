package worker

import (
	"context"
	"errors"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
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
	// 直接使用字符串格式的 CID
	cid := string(body)

	if !util.CheckCid(cid) {
		log.Log.Sugar().Warnf("Warning: wrong unpin cid: %s", cid)
		return nil
	}

	if _, _, err := w.store.Upsert(ctx, cid, nil, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinning
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	}); err != nil {
		log.Log.Sugar().Errorf("Failed to update record status: %v", err)
		return err
	}

	ctxUnpin := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.UnpinTimeout > 0 {
		ctxUnpin, cancel = context.WithTimeout(ctx, w.cfg.Workers.UnpinTimeout)
		defer cancel()
	}

	start := time.Now()
	err := w.ipfs.PinRm(ctxUnpin, cid)
	monitor.ObserveOperation(monitor.OpPinRm, time.Since(start), err)
	if err != nil {
		if err.Error() == "not pinned or pinned indirectly" {
			log.Log.Sugar().Infof("CID %s is already unpinned", cid)
			if _, _, err = w.store.Upsert(ctx, cid, nil, func(r *store.PinRecord) error {
				r.Status = store.StatusUnpinSucceeded
				r.UnpinSucceededAt = time.Now().UnixMilli()
				return nil
			}); err != nil {
				log.Log.Sugar().Errorf("Failed to update record status: %v", err)
				return err
			}

			if err = w.store.DeleteExpireIndex(ctx, cid); err != nil {
				log.Log.Sugar().Errorf("Failed to delete expire index for %s: %v", cid, err)
			}
			return nil
		}

		return w.handleUnpinError(ctx, cid, err)
	}

	if _, _, err = w.store.Upsert(ctx, cid, nil, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinSucceeded
		r.UnpinSucceededAt = time.Now().UnixMilli()
		return nil
	}); err != nil {
		log.Log.Sugar().Errorf("Failed to update record status: %v", err)
		return err
	}

	if err = w.store.DeleteExpireIndex(ctx, cid); err != nil {
		log.Log.Sugar().Errorf("Failed to delete expire index for %s: %v", cid, err)
	}

	return nil
}

func (w *UnpinWorker) handleUnpinError(ctx context.Context, cid string, err error) error {
	log.Log.Sugar().Errorf("Unpin operation failed for %s: %v", cid, err)

	if e := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.UnpinAttemptCount++
		if r.UnpinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
			return nil
		}
		r.Status = store.StatusScheduledForUnpin
		return nil
	}); e != nil {
		return e
	}

	rec, _ := w.store.Get(ctx, cid)
	if rec != nil && rec.UnpinAttemptCount < int32(w.cfg.Workers.MaxRetries) {
		return errors.New("retry unpin")
	}
	return nil
}
