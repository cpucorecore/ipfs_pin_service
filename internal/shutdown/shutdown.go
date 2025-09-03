package shutdown

import (
	"context"
	"sync"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/log"
)

type Manager struct {
	mu           sync.RWMutex
	draining     bool
	shutdownCtx  context.Context
	drainCancel  context.CancelFunc
	workCancel   context.CancelFunc
	shutdownTime time.Time
	wg           sync.WaitGroup
}

func NewManager() *Manager {
	shutdownCtx, drainCancel := context.WithCancel(context.Background())
	_, workCancel := context.WithCancel(context.Background())

	return &Manager{
		shutdownCtx: shutdownCtx,
		drainCancel: drainCancel,
		workCancel:  workCancel,
	}
}

func (m *Manager) StartDrain() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.draining {
		return
	}

	m.draining = true
	m.shutdownTime = time.Now()
	m.drainCancel()

	log.Log.Sugar().Info("Service entering drain mode - no longer accepting new requests")
	m.DebugWaitGroupStatus()
}

func (m *Manager) IsDraining() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.draining
}

func (m *Manager) ShutdownCtx() context.Context {
	return m.shutdownCtx
}

func (m *Manager) ForceShutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workCancel()
	log.Log.Sugar().Info("Force shutdown initiated - cancelling all tasks")
}

func (m *Manager) WaitForCompletion() {
	log.Log.Sugar().Info("Waiting for all managed tasks to complete...")

	done := make(chan struct{})

	go func() {
		log.Log.Sugar().Info("Waiting for WaitGroup...")
		m.wg.Wait()
		log.Log.Sugar().Info("WaitGroup completed, all managed tasks finished")
		close(done)
	}()

	<-done
	log.Log.Sugar().Info("All managed tasks completed gracefully")
}

func (m *Manager) Add(delta int) {
	m.wg.Add(delta)
}

func (m *Manager) Done() {
	m.wg.Done()
}

func (m *Manager) Wait() {
	m.wg.Wait()
}

func (m *Manager) Go(fn func()) {
	m.wg.Add(1)
	log.Log.Sugar().Info("WaitGroup: added 1, starting goroutine")
	go func() {
		defer func() {
			log.Log.Sugar().Info("WaitGroup: goroutine completed, calling Done")
			m.wg.Done()
		}()
		fn()
	}()
}

func (m *Manager) GetShutdownDuration() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.shutdownTime.IsZero() {
		return 0
	}

	return time.Since(m.shutdownTime)
}

func (m *Manager) DebugWaitGroupStatus() {
	log.Log.Sugar().Info("WaitGroup: checking status...")
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Log.Sugar().Info("WaitGroup: no active goroutines")
	case <-time.After(100 * time.Millisecond):
		log.Log.Sugar().Info("WaitGroup: still has active goroutines")
	}
}
