//go:build integration
// +build integration

package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

// Integration test that requires a local RabbitMQ instance.
// Run with:
//
//	IT_RABBITMQ=1 go test ./internal/queue -run TestDequeueConcurrent_Integration -tags=integration -count=1
func TestDequeueConcurrent_Integration(t *testing.T) {
	if os.Getenv("IT_RABBITMQ") != "1" {
		t.Skip("set IT_RABBITMQ=1 to run integration test")
	}

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:5672/"
	}

	cfg := &config.Config{}
	cfg.RabbitMQ.URL = url
	cfg.RabbitMQ.Prefetch = 1
	cfg.RabbitMQ.Pin.Exchange = "it.pin.exchange"
	cfg.RabbitMQ.Pin.Queue = "it.pin.queue"
	cfg.RabbitMQ.Pin.DLX = "it.pin.dlx"
	cfg.RabbitMQ.Pin.RetryQueue = "it.pin.retry.queue"
	cfg.RabbitMQ.Pin.RetryDelay = time.Second

	// Unpin topology is required by setupTopology; set distinct names
	cfg.RabbitMQ.Unpin.Exchange = "it.unpin.exchange"
	cfg.RabbitMQ.Unpin.Queue = "it.unpin.queue"
	cfg.RabbitMQ.Unpin.DLX = "it.unpin.dlx"
	cfg.RabbitMQ.Unpin.RetryQueue = "it.unpin.retry.queue"
	cfg.RabbitMQ.Unpin.RetryDelay = time.Second

	mq, err := NewRabbitMQ(cfg)
	if err != nil {
		t.Fatalf("new rabbitmq: %v", err)
	}
	defer mq.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		K           = 12
		concurrency = 4
		sleepPerMsg = 200 * time.Millisecond
	)

	done := make(chan struct{}, K)
	handler := func(ctx context.Context, body []byte) error {
		time.Sleep(sleepPerMsg)
		done <- struct{}{}
		return nil
	}

	// Start concurrent consumers
	go func() {
		_ = mq.DequeueConcurrent(ctx, cfg.RabbitMQ.Pin.Queue, concurrency, handler)
	}()

	// Enqueue K messages
	for i := 0; i < K; i++ {
		if err := mq.Enqueue(ctx, cfg.RabbitMQ.Pin.Exchange, []byte("x")); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	start := time.Now()
	for i := 0; i < K; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for handler completion")
		}
	}
	elapsed := time.Since(start)

	serial := time.Duration(K) * sleepPerMsg
	parallel := time.Duration((K+concurrency-1)/concurrency) * sleepPerMsg

	// Allow one extra slice for scheduling jitter
	upperBound := parallel + sleepPerMsg
	if !(elapsed < serial && elapsed <= upperBound) {
		t.Fatalf("elapsed %v, want < serial %v and <= %v (parallel+one), parallel=%v",
			elapsed, serial, upperBound, parallel)
	}
}

// Test error handling causing message to route to retry queue via DLX and then be re-delivered
func TestDequeueConcurrent_ErrorFlow_Integration(t *testing.T) {
	if os.Getenv("IT_RABBITMQ") != "1" {
		t.Skip("set IT_RABBITMQ=1 to run integration test")
	}

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:5672/"
	}

	cfg := &config.Config{}
	cfg.RabbitMQ.URL = url
	cfg.RabbitMQ.Prefetch = 1
	cfg.RabbitMQ.Pin.Exchange = "it.err.pin.exchange"
	cfg.RabbitMQ.Pin.Queue = "it.err.pin.queue"
	cfg.RabbitMQ.Pin.DLX = "it.err.pin.dlx"
	cfg.RabbitMQ.Pin.RetryQueue = "it.err.pin.retry"
	cfg.RabbitMQ.Pin.RetryDelay = 300 * time.Millisecond

	// Unpin topology to satisfy setup
	cfg.RabbitMQ.Unpin.Exchange = "it.err.unpin.exchange"
	cfg.RabbitMQ.Unpin.Queue = "it.err.unpin.queue"
	cfg.RabbitMQ.Unpin.DLX = "it.err.unpin.dlx"
	cfg.RabbitMQ.Unpin.RetryQueue = "it.err.unpin.retry"
	cfg.RabbitMQ.Unpin.RetryDelay = 300 * time.Millisecond

	mq, err := NewRabbitMQ(cfg)
	if err != nil {
		t.Fatalf("new rabbitmq: %v", err)
	}
	defer mq.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	attempts := 0
	done := make(chan struct{})

	handler := func(ctx context.Context, body []byte) error {
		attempts++
		if attempts == 1 {
			// first attempt fails -> message should go to retry queue and come back
			return fmt.Errorf("simulated error")
		}
		// second attempt succeeds
		close(done)
		return nil
	}

	go func() {
		_ = mq.DequeueConcurrent(ctx, cfg.RabbitMQ.Pin.Queue, 1, handler)
	}()

	if err := mq.Enqueue(ctx, cfg.RabbitMQ.Pin.Exchange, []byte("x")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for redelivery, attempts=%d", attempts)
	}

	if attempts < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", attempts)
	}
}
