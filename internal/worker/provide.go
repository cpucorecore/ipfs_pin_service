package worker

import (
	"context"
	"encoding/json"
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

type ProvideWorker struct {
	store store.Store
	queue queue.MessageQueue
	ipfs  *ipfs.Client
	cfg   *config.Config
}

func NewProvideWorker(
	store store.Store,
	queue queue.MessageQueue,
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

func (w *ProvideWorker) Start(ctx context.Context) error {
	return w.queue.DequeueConcurrent(ctx, w.cfg.RabbitMQ.Provide.Queue, w.cfg.Workers.ProvideConcurrency, w.handleProvideMessage)
}

func (w *ProvideWorker) handleProvideMessage(ctx context.Context, body []byte) error {
	var req ProvideRequestMsg
	if err := json.Unmarshal(body, &req); err != nil {
		log.Log.Sugar().Errorf("ProvideWorker parse request[%s] err: %v", string(body), err)
		return err
	}

	if !util.CheckCid(req.Cid) {
		log.Log.Sugar().Warnf("ProvideWorker check cid[%s] fail", req.Cid)
		return nil
	}

	cid := req.Cid

	pinRecord, err := w.store.Get(ctx, cid)
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	if pinRecord == nil || pinRecord.PinSucceededAt == 0 {
		log.Log.Sugar().Warnf("ProvideWorker cid[%s] not pinned yet, skip provide", cid)
		return nil
	}

	if pinRecord.ProvideSucceededAt > 0 {
		log.Log.Sugar().Infof("ProvideWorker cid[%s] already provided, skip", cid)
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

	log.Log.Sugar().Infof("Provide[%s] start", cid)
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.ProvideTimeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.cfg.Workers.ProvideTimeout)
		defer cancel()
	}

	provideStartTime := time.Now()

	var opType string
	if w.cfg.Workers.ProvideRecursive {
		err = w.ipfs.ProvideRecursive(timeoutCtx, cid)
		opType = monitor.OpProvideRecursive
		log.Log.Sugar().Infof("Provide[%s] using recursive mode", cid)
	} else {
		err = w.ipfs.Provide(timeoutCtx, cid)
		opType = monitor.OpProvide
		log.Log.Sugar().Infof("Provide[%s] using non-recursive mode", cid)
	}
	provideEndTime := time.Now()
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}
	duration := provideEndTime.Sub(provideStartTime)
	monitor.ObserveOperation(opType, duration, err)

	log.Log.Sugar().Infof("Provide[%s] ipfs done: %s", cid, duration)

	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.ProvideSucceededAt = provideEndTime.UnixMilli()
		r.ProvideError = ""
		return nil
	})
	if err != nil {
		return w.handleProvideError(ctx, cid, err)
	}

	log.Log.Sugar().Infof("Provide[%s] done", cid)
	return nil
}

var (
	ErrProvideRetry = errors.New("provide retry")
)

func (w *ProvideWorker) handleProvideError(ctx context.Context, cid string, provideErr error) error {
	log.Log.Sugar().Errorf("Provide[%s] err: %v", cid, provideErr)

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

	log.Log.Sugar().Infof("Provide[%s] out of max retry", cid)
	return nil
}
