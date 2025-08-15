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
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/log"
)

type PinWorker struct {
	store  store.Store
	queue  queue.MessageQueue
	ipfs   *ipfs.Client
	policy *ttl.Policy
	cfg    *config.Config
}

func NewPinWorker(
	store store.Store,
	queue queue.MessageQueue,
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

func (w *PinWorker) Start(ctx context.Context) error {
	return w.queue.DequeueConcurrent(ctx, w.cfg.RabbitMQ.Pin.Queue, w.cfg.Workers.PinConcurrency, w.handleMessage)
}

type requestMessage struct {
	Cid  string `json:"cid"`
	Size int64  `json:"size"`
}

func (w *PinWorker) handleMessage(ctx context.Context, body []byte) error {
	log.Log.Sugar().Infof("Received pin message with body length: %d", len(body))
	var req requestMessage
	if err := json.Unmarshal(body, &req); err != nil {
		log.Log.Sugar().Errorf("Failed to unmarshal request message: %v", err)
		return err
	}

	if !util.CheckCid(req.Cid) {
		log.Log.Sugar().Warnf("Warning: wrong pin cid: %s", req.Cid)
		return nil
	}

	cid := req.Cid

	now := time.Now().UnixMilli()

	// Ensure record exists and size is updated using Upsert
	rec, _, err := w.store.Upsert(ctx, cid,
		func(r *store.PinRecord) {
			r.Cid = cid
			r.Status = store.StatusReceived
			r.ReceivedAt = now
			r.LastUpdateAt = now
			r.SizeBytes = req.Size
		},
		func(r *store.PinRecord) error {
			if req.Size > 0 && r.SizeBytes != req.Size {
				r.SizeBytes = req.Size
				r.LastUpdateAt = now
			}
			return nil
		},
	)
	if err != nil {
		log.Log.Sugar().Errorf("Failed to upsert record %s: %v", cid, err)
		return err
	}

	// Determine size for TTL
	size := rec.GetSizeBytes()
	if req.Size > 0 {
		size = req.Size
	}

	// Transition to Pinning
	if _, _, err := w.store.Upsert(ctx, cid, nil, func(r *store.PinRecord) error {
		r.Status = store.StatusPinning
		r.PinStartAt = time.Now().UnixMilli()
		return nil
	}); err != nil {
		log.Log.Sugar().Errorf("Failed to update record status: %v", err)
		return err
	}

	ttl, bucket := w.policy.ComputeWithBucket(size)

	log.Log.Sugar().Infof("Starting pin operation for CID: %s", cid)
	// Add per-operation timeout if configured
	ctxPin := ctx
	var cancel context.CancelFunc
	if w.cfg.Workers.PinTimeout > 0 {
		ctxPin, cancel = context.WithTimeout(ctx, w.cfg.Workers.PinTimeout)
		defer cancel()
	}
	start := time.Now()
	err = w.ipfs.PinAdd(ctxPin, cid)
	monitor.ObserveOperation(monitor.OpPinAdd, time.Since(start), err)
	if err != nil {
		log.Log.Sugar().Errorf("Failed to pin CID %s: %v", cid, err)
		return w.handlePinError(ctx, cid, err)
	}
	log.Log.Sugar().Infof("Successfully pinned CID: %s", cid)

	t := time.Now()
	if _, _, err := w.store.Upsert(ctx, cid, nil, func(r *store.PinRecord) error {
		r.Status = store.StatusActive
		r.PinSucceededAt = t.UnixMilli()
		r.ExpireAt = t.Add(ttl).UnixMilli()
		return nil
	}); err != nil {
		log.Log.Sugar().Errorf("Failed to update record status: %v", err)
		return err
	}

	// Observe file size for successful pin
	monitor.ObserveFileSize(size)
	monitor.ObserveTTLBucket(bucket)

	return nil
}

func (w *PinWorker) handlePinError(ctx context.Context, cid string, err error) error {
	log.Log.Sugar().Errorf("Pin operation failed for %s: %v", cid, err)

	// Update attempt count and decide whether to retry (return error) or stop (ack with dead letter)
	updateErr := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.PinAttemptCount++

		if r.PinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
			return nil
		}

		// keep in Pinning for retry
		r.Status = store.StatusPinning
		return nil
	})
	if updateErr != nil {
		return updateErr
	}
	// If still under max retries, return non-nil to trigger DLX retry
	rec, _ := w.store.Get(ctx, cid)
	if rec != nil && rec.PinAttemptCount < int32(w.cfg.Workers.MaxRetries) {
		return errors.New("retry pin")
	}
	// Max retries reached; ack (nil) and keep DeadLetter status
	return nil
}
