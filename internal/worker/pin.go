package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type PinWorker struct {
	store  store.Store
	queue  mq.Queue
	ipfs   *ipfs.Client
	policy *ttl.Policy
	cfg    *config.Config
}

func NewPinWorker(
	store store.Store,
	queue mq.Queue,
	ipfs *ipfs.Client,
	policy *ttl.Policy,
	cfg *config.Config,
) *PinWorker {
	return &PinWorker{
		store:  store,
		queue:  queue,
		ipfs:   ipfs,
		policy: policy,
		cfg:    cfg,
	}
}

type PinRequestMsg struct {
	Cid  string `json:"cid"`
	Size int64  `json:"size"`
}

func (w *PinWorker) Start() {
	w.queue.StartPinConsumer(w.handlePinMessage)
}

func (w *PinWorker) handlePinMessage(ctx context.Context, body []byte) error {
	var req PinRequestMsg
	if err := json.Unmarshal(body, &req); err != nil {
		log.Log.Sugar().Errorf("PinWorker parse request[%s] err: %v", string(body), err)
		return err
	}

	if !util.CheckCid(req.Cid) {
		log.Log.Sugar().Warnf("PinWorker check cid[%s] fail", req.Cid)
		return nil
	}

	cid := req.Cid
	pinRecord, err := w.store.Get(ctx, cid)
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	if pinRecord == nil {
		pinRecord = &store.PinRecord{
			Cid:        cid,
			Status:     store.StatusReceived,
			Size:       req.Size,
			ReceivedAt: time.Now().UnixMilli(),
		}
		err = w.store.Put(ctx, pinRecord)
		if err != nil {
			return w.handlePinError(ctx, cid, err)
		}
	}
	pinRecord.Size = req.Size

	pinRecord.Status = store.StatusPinning
	pinRecord.PinStartAt = time.Now().UnixMilli()
	err = w.store.Put(ctx, pinRecord)
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	log.Log.Sugar().Infof("Pin[%s] start", cid)
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.PinTimeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.cfg.Workers.PinTimeout)
		defer cancel()
	}
	pinStartTime := time.Now()
	err = w.ipfs.PinAdd(timeoutCtx, cid)
	pinEndTime := time.Now()
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}
	duration := pinEndTime.Sub(pinStartTime)
	monitor.ObserveOperation(monitor.OpPinAdd, duration, err)
	log.Log.Sugar().Infof("Pin[%s] ipfs done: %s", cid, duration)

	ttl, bucket := w.policy.ComputeTTL(pinRecord.Size)
	expireAt := pinEndTime.Add(ttl).UnixMilli()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusActive
		r.PinSucceededAt = pinEndTime.UnixMilli()
		r.ExpireAt = expireAt
		return nil
	})
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	if err = w.store.AddExpireIndex(ctx, cid, expireAt); err != nil {
		log.Log.Sugar().Errorf("Pin[%s] add expire index err: %v", cid, err)
		return w.handlePinError(ctx, cid, err)
	}

	provideBody := []byte(cid)
	err = w.queue.EnqueueProvide(provideBody)
	if err != nil {
		log.Log.Sugar().Errorf("Pin[%s] enqueue provide err: %v", cid, err)
	} else {
		log.Log.Sugar().Infof("Pin[%s] enqueued provide request", cid)
	}

	log.Log.Sugar().Infof("Pin[%s] done", cid)
	monitor.ObserveFileSize(pinRecord.Size)
	monitor.ObserveTTLBucket(bucket)
	return nil
}

var (
	ErrPinRetry = errors.New("pin retry")
)

func (w *PinWorker) handlePinError(ctx context.Context, cid string, pinErr error) error {
	log.Log.Sugar().Errorf("Pin[%s] err: %v", cid, pinErr)

	var pinAttemptCount int32
	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.PinAttemptCount++
		pinAttemptCount = r.PinAttemptCount
		if r.PinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
		}
		return nil
	})
	if err != nil {
		return err
	}

	if pinAttemptCount < int32(w.cfg.Workers.MaxRetries) {
		return ErrPinRetry
	}

	log.Log.Sugar().Infof("Pin[%s] out of max retry", cid)
	return nil
}
