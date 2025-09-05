package worker

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type UnpinWorker struct {
	maxRetry int
	timeout  time.Duration
	store    store.Store
	queue    mq.Queue
	ipfs     *ipfs.Client
}

func NewUnpinWorker(
	maxRetry int,
	timeout time.Duration,
	store store.Store,
	queue mq.Queue,
	ipfs *ipfs.Client,
) *UnpinWorker {
	return &UnpinWorker{
		maxRetry: maxRetry,
		timeout:  timeout,
		store:    store,
		queue:    queue,
		ipfs:     ipfs,
	}
}

func (w *UnpinWorker) Start() {
	w.queue.StartUnpinConsumer(w.handleMsg)
}

func IsDuplicateUnpinError(err error, cid string) bool {
	isDuplicate := strings.Contains(err.Error(), "not pinned or pinned indirectly")
	if isDuplicate {
		log.Log.Warn("duplicate unpin", zap.String("cid", cid))
	}
	return isDuplicate
}

func (w *UnpinWorker) handleMsg(ctx context.Context, body []byte) error {
	cid := string(body)

	if !util.CheckCid(cid) {
		log.Log.Error("check cid fail", zap.String("module", "UnpinWorker"), zap.String("cid", cid))
		return nil
	}

	log.Log.Info(cid,
		zap.String("op", opUnpin),
		zap.String("step", "start"))

	if err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusUnpinning
		r.UnpinStartAt = time.Now().UnixMilli()
		return nil
	}); err != nil {
		return w.handleUnpinError(ctx, cid, err)
	}

	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.timeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.timeout)
		defer cancel()
	}

	log.Log.Info(cid,
		zap.String("op", opUnpin),
		zap.String("step", "ipfs start"))

	startTs := time.Now()
	err := w.ipfs.PinRm(timeoutCtx, cid)
	endTs := time.Now()
	duration := endTs.Sub(startTs)

	if err == nil || IsDuplicateUnpinError(err, cid) {
		log.Log.Info(cid,
			zap.String("op", opUnpin),
			zap.String("step", "end"),
			zap.Duration("duration", duration))
		monitor.ObserveOperation(monitor.OpPinRm, duration, nil)
		return w.updateStoreUnpinSuccess(ctx, cid, endTs)
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
	log.Log.Error(cid,
		zap.String("op", opUnpin),
		zap.String("step", "err"),
		zap.Error(unpinErr))

	var unpinAttemptCount int32
	if err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.UnpinAttemptCount++
		unpinAttemptCount = r.UnpinAttemptCount
		if r.UnpinAttemptCount >= int32(w.maxRetry) {
			r.Status = store.StatusDeadLetter
		}
		return nil
	}); err != nil {
		return err
	}

	if unpinAttemptCount < int32(w.maxRetry) {
		return ErrUnpinRetry
	}

	log.Log.Error(cid,
		zap.String("op", opUnpin),
		zap.String("step", "err"),
		zap.Error(ErrOutOfMaxRetry))
	return nil
}
