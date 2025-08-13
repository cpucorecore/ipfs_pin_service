package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
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
	log.Printf("Received pin message with body length: %d", len(body))
	var req requestMessage
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("Failed to unmarshal request message: %v", err)
		return err
	}

	if !util.CheckCid(req.Cid) {
		log.Printf("Warning: wrong pin cid: %s", req.Cid)
		return nil
	}

	cid := req.Cid

	now := time.Now().UnixMilli()

	// Upsert record to ensure existence and size availability
	rec, err := w.store.Get(ctx, cid)
	if err != nil {
		log.Printf("Failed to get record %s: %v", cid, err)
		return err
	}
	if rec == nil {
		rec = &store.PinRecord{
			Cid:          cid,
			Status:       store.StatusReceived,
			ReceivedAt:   now,
			LastUpdateAt: now,
			SizeBytes:    req.Size,
		}
		if err := w.store.Put(ctx, rec); err != nil {
			log.Printf("Failed to upsert record %s: %v", cid, err)
			return err
		}
	} else if req.Size > 0 && rec.SizeBytes != req.Size {
		if err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
			r.SizeBytes = req.Size
			r.LastUpdateAt = now
			return nil
		}); err != nil {
			log.Printf("Failed to update size for %s: %v", cid, err)
			return err
		}
	}

	// Determine size for TTL
	size := rec.GetSizeBytes()
	if req.Size > 0 {
		size = req.Size
	}

	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusPinning
		r.PinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	ttl, bucket := w.policy.ComputeWithBucket(size)

	log.Printf("Starting pin operation for CID: %s", cid)
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
		log.Printf("Failed to pin CID %s: %v", cid, err)
		return w.handlePinError(ctx, cid, err)
	}
	log.Printf("Successfully pinned CID: %s", cid)

	t := time.Now()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusActive
		r.PinSucceededAt = t.UnixMilli()
		r.ExpireAt = t.Add(ttl).UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// Observe file size for successful pin
	monitor.ObserveFileSize(size)
	monitor.ObserveTTLBucket(bucket)

	return nil
}

func (w *PinWorker) handlePinError(ctx context.Context, cid string, err error) error {
	log.Printf("Pin operation failed for %s: %v", cid, err)

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
