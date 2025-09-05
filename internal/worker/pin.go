package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type PinWorker struct {
	maxRetry  int
	timeout   time.Duration
	store     store.Store
	queue     mq.Queue
	ipfsCli   *ipfs.Client
	ttlPolicy *ttl.Policy
}

func NewPinWorker(
	maxRetry int,
	timeout time.Duration,
	store store.Store,
	queue mq.Queue,
	ipfsCli *ipfs.Client,
	ttlPolicy *ttl.Policy,
) *PinWorker {
	return &PinWorker{
		maxRetry:  maxRetry,
		timeout:   timeout,
		store:     store,
		queue:     queue,
		ipfsCli:   ipfsCli,
		ttlPolicy: ttlPolicy,
	}
}

type PinMsg struct {
	Cid  string `json:"cid"`
	Size int64  `json:"size"`
}

func (w *PinWorker) Start() {
	w.queue.StartPinConsumer(w.handleMsg)
}

func (w *PinWorker) handleMsg(ctx context.Context, data []byte) error {
	pinMsg := &PinMsg{}
	if err := json.Unmarshal(data, pinMsg); err != nil {
		log.Log.Error("json.Unmarshal err", zap.String("module", "PinWorker"), zap.String("data", string(data)), zap.Error(err))
		return err
	}

	if !util.CheckCid(pinMsg.Cid) {
		log.Log.Warn("check cid fail", zap.String("module", "PinWorker"), zap.String("cid", pinMsg.Cid))
		return nil
	}

	cid := pinMsg.Cid
	size := pinMsg.Size
	pinRecord, err := w.store.Get(ctx, cid)
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	if pinRecord == nil {
		pinRecord = &store.PinRecord{
			Cid:        cid,
			Status:     store.StatusReceived,
			Size:       size,
			ReceivedAt: time.Now().UnixMilli(),
		}
		err = w.store.Put(ctx, pinRecord)
		if err != nil {
			return w.handlePinError(ctx, cid, err)
		}
	}

	pinRecord.Size = size
	pinRecord.Status = store.StatusPinning
	pinRecord.PinStartAt = time.Now().UnixMilli()
	err = w.store.Put(ctx, pinRecord)
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	log.Log.Info(cid,
		zap.String("op", opPin),
		zap.String("step", "start"))
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if w.timeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, w.timeout)
		defer cancel()
	}
	log.Log.Info(cid,
		zap.String("op", opPin),
		zap.String("step", "ipfs start"))

	startTs := time.Now()
	err = w.ipfsCli.PinAdd(timeoutCtx, cid)
	endTs := time.Now()
	duration := endTs.Sub(startTs)
	monitor.ObserveOperation(monitor.OpPinAdd, duration, err)
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	log.Log.Info(cid,
		zap.String("op", opPin),
		zap.String("step", "ipfs end"),
		zap.Duration("duration", duration))

	ttl, bucket := w.ttlPolicy.ComputeTTL(pinRecord.Size)
	expireTs := endTs.Add(ttl).UnixMilli()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusActive
		r.PinSucceededAt = endTs.UnixMilli()
		r.ExpireAt = expireTs
		return nil
	})
	if err != nil {
		return w.handlePinError(ctx, cid, err)
	}

	if err = w.store.AddExpireIndex(ctx, cid, expireTs); err != nil {
		log.Log.Error("add expire index err", zap.String("cid", cid), zap.Error(err))
		return w.handlePinError(ctx, cid, err)
	}

	err = w.queue.EnqueueProvide([]byte(cid))
	if err != nil {
		log.Log.Error("enqueue provide queue err", zap.String("cid", cid), zap.Error(err))
	} else {
		log.Log.Info(cid,
			zap.String("op", opPin),
			zap.String("step", "enqueue provide queue"))
	}

	log.Log.Info(cid,
		zap.String("op", opPin),
		zap.String("step", "end"),
		zap.Int64("size", size),
		zap.String("bucket", bucket))
	monitor.ObserveFileSize(size)
	monitor.ObserveTTLBucket(bucket)
	return nil
}

var (
	ErrPinRetry = errors.New("pin retry")
)

func (w *PinWorker) handlePinError(ctx context.Context, cid string, pinErr error) error {
	log.Log.Error(cid,
		zap.String("op", opPin),
		zap.String("step", "err"),
		zap.Error(pinErr))

	var pinAttemptCount int32
	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.PinAttemptCount++
		pinAttemptCount = r.PinAttemptCount
		if r.PinAttemptCount >= int32(w.maxRetry) {
			r.Status = store.StatusDeadLetter
		}
		return nil
	})
	if err != nil {
		return err
	}

	if pinAttemptCount < int32(w.maxRetry) {
		return ErrPinRetry
	}

	log.Log.Error(cid,
		zap.String("op", opPin),
		zap.String("step", "err"),
		zap.Error(ErrOutOfMaxRetry))
	return nil
}
