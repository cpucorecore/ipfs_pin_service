package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
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
	return w.queue.Dequeue(ctx, w.cfg.RabbitMQ.Pin.Queue, w.handleMessage)
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

	ttl := w.policy.Compute(size)

	log.Printf("Starting pin operation for CID: %s", cid)
	if err = w.ipfs.PinAdd(ctx, cid); err != nil {
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

	return nil
}

func (w *PinWorker) handlePinError(ctx context.Context, cid string, err error) error {
	log.Printf("Pin operation failed for %s: %v", cid, err)

	return w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.PinAttemptCount++

		if r.PinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = store.StatusDeadLetter
			return nil
		}

		r.Status = store.StatusPinning
		return nil
	})
}
