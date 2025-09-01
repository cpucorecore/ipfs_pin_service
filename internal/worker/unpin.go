package worker

import (
	"context"
	"errors"
	"strings"
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

func IsDuplicateUnpinError(err error, cid string) bool {
	isDuplicate := strings.Contains(err.Error(), "not pinned or pinned indirectly")
	if isDuplicate {
		log.Log.Sugar().Warnf("Unpin[%s] duplicate unpin", cid)
	}
	return isDuplicate
}

func (w *UnpinWorker) handleMessage(ctx context.Context, body []byte) error {
	cid := string(body)
	log.Log.Sugar().Infof("Unpin[%s] start", cid)

	if !util.CheckCid(cid) {
		log.Log.Sugar().Warnf("Unpin[%s] wrong cid", cid)
		return nil
	}

	if err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinning
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	}); err != nil {
		log.Log.Sugar().Errorf("Unpin[%s] update status err: %v", cid, err)
		return err
	}

	ctxUnpin := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.UnpinTimeout > 0 {
		ctxUnpin, cancel = context.WithTimeout(ctx, w.cfg.Workers.UnpinTimeout)
		defer cancel()
	}

	unpinStartTime := time.Now()
	err := w.ipfs.PinRm(ctxUnpin, cid)
	unpinEndTime := time.Now()
	duration := unpinEndTime.Sub(unpinStartTime)
	if err == nil || IsDuplicateUnpinError(err, cid) {
		log.Log.Sugar().Infof("Unpin[%s] finish in %s", cid, duration)
		monitor.ObserveOperation(monitor.OpPinRm, duration, nil)
		return w.updateStoreUnpinSuccess(ctx, cid, unpinEndTime)
	}

	return w.handleUnpinError(ctx, cid, err)
}

func (w *UnpinWorker) updateStoreUnpinSuccess(ctx context.Context, cid string, timestamp time.Time) error {
	return w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinSucceeded
		r.UnpinSucceededAt = timestamp.UnixMilli()
		return nil
	})
}

var (
	ErrUnpinRetry = errors.New("unpin retry")
)

func (w *UnpinWorker) handleUnpinError(ctx context.Context, cid string, unpinErr error) error {
	log.Log.Sugar().Errorf("Unpin[%s] fail with err: %v", cid, unpinErr)

	var unpinAttemptCount int32
	if err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.UnpinAttemptCount++
		unpinAttemptCount = r.UnpinAttemptCount
		if r.UnpinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
		}
		return nil
	}); err != nil {
		return err
	}

	if unpinAttemptCount < int32(w.cfg.Workers.MaxRetries) {
		return ErrUnpinRetry
	}

	log.Log.Sugar().Warnf("Unpin[%s] out of max retry", cid)
	return nil
}
