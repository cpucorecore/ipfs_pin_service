package worker

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type ProvideFunc func(ctx context.Context, cid string) error

type ProvideWorker struct {
	maxRetry    int
	timeout     time.Duration
	provideFunc ProvideFunc
	opType      string
	store       store.Store
	queue       mq.Queue
	ipfsCli     *ipfs.Client
}

func NewProvideWorker(
	maxRetry int,
	timeout time.Duration,
	provideRecursive bool,
	store store.Store,
	queue mq.Queue,
	ipfsCli *ipfs.Client,
) *ProvideWorker {
	log.Log.Info("provide mode", zap.Bool("provideRecursive", provideRecursive))

	pw := &ProvideWorker{
		maxRetry: maxRetry,
		timeout:  timeout,
		store:    store,
		queue:    queue,
		ipfsCli:  ipfsCli,
	}

	if provideRecursive {
		pw.provideFunc = pw.ProvideRecursive
		pw.opType = monitor.OpProvideRecursive
	} else {
		pw.provideFunc = pw.Provide
		pw.opType = monitor.OpProvide
	}

	return pw
}

func (w *ProvideWorker) Provide(ctx context.Context, cid string) error {
	return w.ipfsCli.Provide(ctx, cid)
}

func (w *ProvideWorker) ProvideRecursive(ctx context.Context, cid string) error {
	return w.ipfsCli.ProvideRecursive(ctx, cid)
}

func (w *ProvideWorker) Start() {
	w.queue.StartProvideConsumer(w.handleMsg)
}

func (w *ProvideWorker) handleMsg(ctx context.Context, body []byte) error {
	cid := string(body)

	if !util.CheckCid(cid) {
		log.Log.Error("check cid fail", zap.String("module", "ProvideWorker"), zap.String("cid", cid))
		return nil
	}

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "start"))

	pinRecord, err := w.store.Get(ctx, cid)
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	if pinRecord == nil || pinRecord.PinSucceededAt == 0 {
		log.Log.Error("cid not pinned, skip provide", zap.String("cid", cid))
		return nil
	}

	now := time.Now()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideStartAt = now.UnixMilli()
		return nil
	})
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.timeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.timeout)
		defer cancel()
	}

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "ipfs start"))

	startTs := time.Now()
	err = w.provideFunc(timeoutCtx, cid)
	endTs := time.Now()
	duration := endTs.Sub(startTs)
	monitor.ObserveOperation(w.opType, duration, err)
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	log.Log.Info(cid,
		zap.String("op", "provide"),
		zap.String("step", "ipfs end"),
		zap.Duration("duration", duration))

	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideSucceededAt = endTs.UnixMilli()
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
		if r.ProvideAttemptCount >= int32(w.maxRetry) {
			r.ProvideError = provideErr.Error()
		}
		return nil
	})
	if err != nil {
		return err
	}

	if provideAttemptCount < int32(w.maxRetry) {
		return ErrProvideRetry
	}

	log.Log.Error(cid,
		zap.String("op", "provide"),
		zap.String("step", "err"),
		zap.Error(ErrOutOfMaxRetry))
	return nil
}
