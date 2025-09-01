package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func newTempPebble(t *testing.T) *PebbleStore {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &PebbleStore{db: db}
}

func TestPebbleStorePutGet(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	rec := &PinRecord{
		Cid:        "bafy-1",
		Status:     StatusActive,
		ReceivedAt: time.Now().Unix(),
		ExpireAt:   0,
	}

	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	got, err := s.Get(ctx, rec.Cid)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got == nil || got.Cid != rec.Cid || got.Status != rec.Status {
		t.Fatalf("Get mismatch: got %+v", got)
	}
}

func TestPebbleStoreGetNotFound(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()
	got, err := s.Get(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing record, got: %+v", got)
	}
}

func TestPebbleStoreUpdate(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	rec := &PinRecord{Cid: "bafy-2", Status: StatusActive, ExpireAt: 0}
	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := s.Update(ctx, rec.Cid, func(pr *PinRecord) error {
		pr.Status = StatusPinSucceeded
		pr.ExpireAt = 100
		return nil
	})
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}

	got, err := s.Get(ctx, rec.Cid)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got.Status != StatusPinSucceeded || got.ExpireAt != 100 {
		t.Fatalf("Update not applied: %+v", got)
	}
}

func TestPebbleStoreUpdateMissing(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()
	err := s.Update(ctx, "missing", func(pr *PinRecord) error { return nil })
	if err == nil {
		t.Fatalf("expected error for missing record")
	}
}

func TestAddExpireIndex(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	cid := "test-cid"
	expireAt := int64(123456789)

	// Test adding expire index
	err := s.AddExpireIndex(ctx, cid, expireAt)
	if err != nil {
		t.Fatalf("AddExpireIndex error: %v", err)
	}

	// Verify the index was added by querying it
	cids, err := s.GetExpireCids(ctx, expireAt+1000, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 1 || cids[0] != cid {
		t.Fatalf("expected to find cid %s, got %v", cid, cids)
	}
}

func TestDeleteExpireIndex(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	cid := "test-cid"
	expireAt := int64(123456789)

	// First add the expire index
	err := s.AddExpireIndex(ctx, cid, expireAt)
	if err != nil {
		t.Fatalf("AddExpireIndex error: %v", err)
	}

	// Verify it was added
	cids, err := s.GetExpireCids(ctx, expireAt+1000, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error after add: %v", err)
	}
	if len(cids) != 1 || cids[0] != cid {
		t.Fatalf("expected to find cid %s after add, got %v", cid, cids)
	}

	// Now delete the expire index
	err = s.DeleteExpireIndex(ctx, cid, expireAt)
	if err != nil {
		t.Fatalf("DeleteExpireIndex error: %v", err)
	}

	// Verify it was deleted
	cids, err = s.GetExpireCids(ctx, expireAt+1000, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error after delete: %v", err)
	}
	if len(cids) != 0 {
		t.Fatalf("expected no cids after delete, got %v", cids)
	}
}

func TestIndexByExpireBefore(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// Two records expiring at 100 and 200, one with no expire
	recs := []*PinRecord{
		{Cid: "x1", Status: StatusActive, ExpireAt: 100},
		{Cid: "x2", Status: StatusActive, ExpireAt: 200},
		{Cid: "x3", Status: StatusActive, ExpireAt: 0},
	}
	for _, r := range recs {
		if err := s.Put(ctx, r); err != nil {
			t.Fatalf("Put error: %v", err)
		}
		// Add expire index since Put no longer does it automatically
		if r.ExpireAt > 0 {
			if err := s.AddExpireIndex(ctx, r.Cid, r.ExpireAt); err != nil {
				t.Fatalf("AddExpireIndex error: %v", err)
			}
		}
	}

	cids, err := s.GetExpireCids(ctx, 150, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 1 || cids[0] != "x1" {
		t.Fatalf("unexpected result: %v", cids)
	}

	cids, err = s.GetExpireCids(ctx, 300, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 2 || cids[0] != "x1" || cids[1] != "x2" {
		t.Fatalf("unexpected result: %v", cids)
	}

	// limit should restrict count
	cids, err = s.GetExpireCids(ctx, 300, 1)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 1 {
		t.Fatalf("unexpected limited result: %v", cids)
	}
}

func TestPebbleStoreClose(t *testing.T) {
	// Ensure Close works without errors on temp db
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("NewPebbleStore error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	// second close via underlying db should be a no-op due to new store instance using Open
	// verify directory exists still
	if _, err := os.Stat(filepath.Clean(dir)); err != nil {
		t.Fatalf("temp dir should exist: %v", err)
	}
}

func TestGetExpireCidsEmpty(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// Test querying empty store
	cids, err := s.GetExpireCids(ctx, 1000, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 0 {
		t.Fatalf("expected empty result, got %v", cids)
	}
}

func TestGetExpireCidsLimit(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// Add multiple expire indexes
	for i := 0; i < 5; i++ {
		cid := fmt.Sprintf("test-cid-%d", i)
		expireAt := int64(1000 + i)
		err := s.AddExpireIndex(ctx, cid, expireAt)
		if err != nil {
			t.Fatalf("AddExpireIndex error for %s: %v", cid, err)
		}
	}

	// Test limit
	cids, err := s.GetExpireCids(ctx, 2000, 3)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	if len(cids) != 3 {
		t.Fatalf("expected 3 results with limit, got %d: %v", len(cids), cids)
	}
}

func TestGetExpireCidsOrderingAndLimit(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// Insert with expire ts and out-of-order cids for tie-breaker
	items := []PinRecord{
		{Cid: "c-b", Status: StatusActive, ExpireAt: 2000},
		{Cid: "c-z", Status: StatusActive, ExpireAt: 1000},
		{Cid: "c-a", Status: StatusActive, ExpireAt: 1000},
		{Cid: "c-c", Status: StatusActive, ExpireAt: 3000},
	}
	for i := range items {
		rec := items[i]
		if err := s.Put(ctx, &rec); err != nil {
			t.Fatalf("Put error: %v", err)
		}
		// Add expire index since Put no longer does it automatically
		if rec.ExpireAt > 0 {
			if err := s.AddExpireIndex(ctx, rec.Cid, rec.ExpireAt); err != nil {
				t.Fatalf("AddExpireIndex error: %v", err)
			}
		}
	}

	// Without tight upper bound to include all
	got, err := s.GetExpireCids(ctx, 9000, 10)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	expected := []string{"c-a", "c-z", "c-b", "c-c"}
	if len(got) != len(expected) {
		t.Fatalf("unexpected count: got %d want %d (%v)", len(got), len(expected), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("expire order mismatch at %d: got %v want %v", i, got, expected)
		}
	}

	// Limit should respect the same ordering
	got, err = s.GetExpireCids(ctx, 9000, 2)
	if err != nil {
		t.Fatalf("GetExpireCids error: %v", err)
	}
	expectedLimited := []string{"c-a", "c-z"}
	if len(got) != len(expectedLimited) || got[0] != expectedLimited[0] || got[1] != expectedLimited[1] {
		t.Fatalf("unexpected limited order: got %v want %v", got, expectedLimited)
	}
}

func newTestStore(t *testing.T) *PebbleStore {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	return &PebbleStore{db: db}
}
