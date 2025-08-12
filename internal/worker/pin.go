package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	pb "github.com/cpucorecore/ipfs_pin_service/proto"
	"google.golang.org/protobuf/proto"
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

func (w *PinWorker) handleMessage(ctx context.Context, body []byte) error {
	log.Printf("Received pin message with body length: %d", len(body))
	pbRec := &pb.PinRecord{}
	if err := proto.Unmarshal(body, pbRec); err != nil {
		log.Printf("Failed to unmarshal record: %v", err)
		return err
	}
	cid := pbRec.Cid

	err := w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusPinning
		r.PinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	ttl := w.policy.Compute(pbRec.SizeBytes)

	log.Printf("Starting pin operation for CID: %s", cid)
	if err = w.ipfs.PinAdd(ctx, cid); err != nil {
		log.Printf("Failed to pin CID %s: %v", cid, err)
		return w.handlePinError(ctx, cid, err)
	}
	log.Printf("Successfully pinned CID: %s", cid)

	now := time.Now()
	err = w.store.Update(ctx, cid, func(r *store.PinRecord) error {
		r.Status = store.StatusActive
		r.PinSucceededAt = now.UnixMilli()
		r.ExpireAt = now.Add(ttl).UnixMilli()
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
