package worker

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type ProvideWorker struct {
	store store.Store
	queue mq.Queue
	ipfs  *ipfs.Client
	cfg   *config.Config
}

func NewProvideWorker(
	store store.Store,
	queue mq.Queue,
	ipfs *ipfs.Client,
	cfg *config.Config,
) *ProvideWorker {
	return &ProvideWorker{
		store: store,
		queue: queue,
		ipfs:  ipfs,
		cfg:   cfg,
	}
}

func (w *ProvideWorker) Start() {
	w.queue.StartProvideConsumer(w.handleProvideMessage)
}

func (w *ProvideWorker) handleProvideMessage(ctx context.Context, body []byte) error {
	cid := string(body)

	if !util.CheckCid(cid) {
		log.Log.Warn("check cid fail", zap.String("module", "ProvideWorker"), zap.String("cid", cid))
		return nil
	}

	pinRecord, err := w.store.Get(ctx, cid)
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	if pinRecord == nil || pinRecord.PinSucceededAt == 0 {
		log.Log.Warn("cid not pinned yet, skip provide", zap.String("cid", cid))
		return nil
	}

	if pinRecord.ProvideSucceededAt > 0 {
		log.Log.Info("cid already provided, skip", zap.String("cid", cid))
		return nil
	}

	pinRecord.ProvideStartAt = time.Now().UnixMilli()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideStartAt = pinRecord.ProvideStartAt
		r.ProvideAttemptCount++
		return nil
	})
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "start"))
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.ProvideTimeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.cfg.Workers.ProvideTimeout)
		defer cancel()
	}

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "ipfs start"))

	provideStartTime := time.Now()

	var opType string
	var mode string
	if w.cfg.Workers.ProvideRecursive {
		err = w.ipfs.ProvideRecursive(timeoutCtx, cid)
		opType = monitor.OpProvideRecursive
		mode = "recursive"
	} else {
		err = w.ipfs.Provide(timeoutCtx, cid)
		opType = monitor.OpProvide
		mode = "non-recursive"
	}
	provideEndTime := time.Now()
	duration := provideEndTime.Sub(provideStartTime)

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "ipfs end"),
		zap.String("mode", mode),
		zap.Duration("duration", duration))

	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}
	monitor.ObserveOperation(opType, duration, err)

	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideSucceededAt = provideEndTime.UnixMilli()
		r.ProvideError = ""
		return nil
	})
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	return nil
}

var (
	ErrProvideRetry = errors.New("provide retry")
)

func (w *ProvideWorker) handleProvideError(ctx context.Context, cid string, provideErr error) error {
	log.Log.Error(cid,
		zap.String("op", "provide"),
		zap.String("step", "err"),
		zap.Error(provideErr))

	var provideAttemptCount int32
	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideAttemptCount++
		provideAttemptCount = r.ProvideAttemptCount
		if r.ProvideAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.ProvideError = provideErr.Error()
		}
		return nil
	})
	if err != nil {
		return err
	}

	if provideAttemptCount < int32(w.cfg.Workers.MaxRetries) {
		return ErrProvideRetry
	}

	log.Log.Info("out of max retry", zap.String("cid", cid))
	return nil
}
