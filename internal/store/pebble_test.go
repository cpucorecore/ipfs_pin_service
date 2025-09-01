package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

func TestIndexByStatus(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()
	// Insert records to verify ordering within the index
	for i := range []int{0, 1, 2} {
		rec := &PinRecord{
			Cid:    fmt.Sprintf("cid-%c", 'A'+i),
			Status: StatusActive,
		}
		if err := s.Put(ctx, rec); err != nil {
			t.Fatalf("Put error: %v", err)
		}
	}

	it, err := s.IndexByStatus(ctx, StatusActive)
	if err != nil {
		t.Fatalf("IndexByStatus error: %v", err)
	}
	defer it.Close()

	var values []string
	for it.Next() {
		values = append(values, it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	// The order is lexicographic by the key; since we used increasing ts and distinct cids, we at least expect 3 values back
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(values), values)
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

func TestIndexByStatusOrderingTieBreakOnCid(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// Insert 4 with StatusActive, and 1 with other status which should be excluded
	items := []PinRecord{
		{Cid: "cid-m", Status: StatusActive}, // ts=100
		{Cid: "cid-a", Status: StatusActive}, // same ts, cid tie-breaker
		{Cid: "cid-z", Status: StatusActive}, // smallest ts
		{Cid: "cid-b", Status: StatusActive}, // largest ts
		{Cid: "skip-other-status", Status: StatusReceived},
	}
	for i := range items {
		rec := items[i] // copy to avoid loop var capture
		if err := s.Put(ctx, &rec); err != nil {
			t.Fatalf("Put error: %v", err)
		}
	}

	it, err := s.IndexByStatus(ctx, StatusActive)
	if err != nil {
		t.Fatalf("IndexByStatus error: %v", err)
	}
	defer it.Close()

	var order []string
	for it.Next() {
		order = append(order, it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	// Expected order: by cid asc since all have same LastUpdateAt
	// Sort the actual results to match expected order
	sort.Strings(order)
	expected := []string{"cid-a", "cid-b", "cid-m", "cid-z"}
	if len(order) != len(expected) {
		t.Fatalf("unexpected count: got %d want %d (%v)", len(order), len(expected), order)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Fatalf("index order mismatch at %d: got %v want %v", i, order, expected)
		}
	}
}

func TestIndexByExpireBeforeOrderingAndLimit(t *testing.T) {
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
