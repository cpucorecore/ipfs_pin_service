package worker

import (
	"context"
	"log"
	"time"

	pb "github.com/cpucorecore/ipfs_pin_service/proto"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/model"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
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
	rec := &model.PinRecord{PinRecord: pbRec}

	// 更新状态为 Pinning
	err := w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.Status = int32(model.StatusPinning)
		r.PinStartAt = time.Now().UnixMilli()
		return nil
	})
	if err != nil {
		log.Printf("Failed to update record status: %v", err)
		return err
	}

	// 计算 TTL（如果没有大小信息，使用默认 TTL）
	ttl := w.policy.Compute(rec.SizeBytes)

	// 执行 pin
	log.Printf("Starting pin operation for CID: %s", rec.Cid)
	if err := w.ipfs.PinAdd(ctx, rec.Cid); err != nil {
		log.Printf("Failed to pin CID %s: %v", rec.Cid, err)
		return w.handlePinError(ctx, rec, err)
	}
	log.Printf("Successfully pinned CID: %s", rec.Cid)

	// 更新状态为 Active
	now := time.Now()
	err = w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.Status = int32(model.StatusActive)
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

func (w *PinWorker) handlePinError(ctx context.Context, rec *model.PinRecord, err error) error {
	log.Printf("Pin operation failed for %s: %v", rec.Cid, err)

	return w.store.Update(ctx, rec.Cid, func(r *model.PinRecord) error {
		r.PinAttemptCount++

		if r.PinAttemptCount >= int32(w.cfg.Workers.MaxRetries) {
			r.Status = int32(model.StatusDeadLetter)
			return nil
		}

		// 重试
		r.Status = int32(model.StatusQueuedForPin)
		return nil
	})
}
