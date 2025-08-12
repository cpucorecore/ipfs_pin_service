package worker

import (
	"context"
	"log"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
)

type TTLChecker struct {
	store store.Store
	queue queue.MessageQueue
	cfg   *config.Config
}

func NewTTLChecker(
	store store.Store,
	queue queue.MessageQueue,
	cfg *config.Config,
) *TTLChecker {
	return &TTLChecker{
		store: store,
		queue: queue,
		cfg:   cfg,
	}
}

func (c *TTLChecker) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.TTLChecker.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.checkExpiredRecords(ctx); err != nil {
				log.Printf("TTL check failed: %v", err)
			}
		}
	}
}

func (c *TTLChecker) checkExpiredRecords(ctx context.Context) error {
	now := time.Now().UnixMilli()
	expiredRecords, err := c.store.IndexByExpireBefore(ctx, now, c.cfg.TTLChecker.BatchSize)
	if err != nil {
		log.Printf("Failed to get expired records: %v", err)
		return err
	}

	log.Printf("ttl: Found %d expired records", len(expiredRecords))

	processedCount := 0
	for _, cid := range expiredRecords {
		rec, err := c.store.Get(ctx, cid)
		if err != nil {
			log.Printf("Failed to get record %s: %v", cid, err)
			continue
		}
		if rec == nil {
			continue
		}

		// Only process Active records
		if store.Status(rec.Status) != store.StatusActive {
			continue
		}

		// Mark as ScheduledForUnpin
		err = c.store.Update(ctx, cid, func(r *store.PinRecord) error {
			r.Status = store.StatusScheduledForUnpin
			r.ScheduleUnpinAt = now
			return nil
		})
		if err != nil {
			log.Printf("Failed to update record %s: %v", cid, err)
			continue
		}

		// Enqueue only CID to unpin queue
		if err := c.queue.Enqueue(ctx, c.cfg.RabbitMQ.Unpin.Exchange, []byte(cid)); err != nil {
			log.Printf("Failed to enqueue record %s: %v", cid, err)
			continue
		}

		processedCount++
		log.Printf("Scheduled record %s for unpin", cid)
	}

	if processedCount > 0 {
		log.Printf("TTL check completed: processed %d expired records", processedCount)
	}

	return nil
}
