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
		Cid:          "bafy-1",
		Status:       StatusActive,
		ReceivedAt:   time.Now().Unix(),
		LastUpdateAt: time.Now().Unix(),
		ExpireAt:     0,
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

	now := time.Now().Unix()
	rec := &PinRecord{Cid: "bafy-2", Status: StatusActive, LastUpdateAt: now, ExpireAt: 0}
	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := s.Update(ctx, rec.Cid, func(pr *PinRecord) error {
		pr.Status = StatusPinSucceeded
		pr.LastUpdateAt = now + 10
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
	if got.Status != StatusPinSucceeded || got.LastUpdateAt != now+10 || got.ExpireAt != 100 {
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
	base := time.Now().Unix()

	// Insert with different timestamps to verify ordering within the index is by ts then cid
	for i, ts := range []int64{base + 1, base + 2, base + 3} {
		rec := &PinRecord{
			Cid:          fmt.Sprintf("cid-%c", 'A'+i),
			Status:       StatusActive,
			LastUpdateAt: ts,
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
		{Cid: "x1", Status: StatusActive, LastUpdateAt: 1, ExpireAt: 100},
		{Cid: "x2", Status: StatusActive, LastUpdateAt: 2, ExpireAt: 200},
		{Cid: "x3", Status: StatusActive, LastUpdateAt: 3, ExpireAt: 0},
	}
	for _, r := range recs {
		if err := s.Put(ctx, r); err != nil {
			t.Fatalf("Put error: %v", err)
		}
	}

	cids, err := s.IndexByExpireBefore(ctx, 150, 10)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
	}
	if len(cids) != 1 || cids[0] != "x1" {
		t.Fatalf("unexpected result: %v", cids)
	}

	cids, err = s.IndexByExpireBefore(ctx, 300, 10)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
	}
	if len(cids) != 2 || cids[0] != "x1" || cids[1] != "x2" {
		t.Fatalf("unexpected result: %v", cids)
	}

	// limit should restrict count
	cids, err = s.IndexByExpireBefore(ctx, 300, 1)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
	}
	if len(cids) != 1 {
		t.Fatalf("unexpected limited result: %v", cids)
	}
}

func TestDeleteExpireIndex(t *testing.T) {
	s := newTempPebble(t)
	ctx := context.Background()

	// record without expire: should be no-op
	if err := s.Put(ctx, &PinRecord{Cid: "n1", Status: StatusActive, LastUpdateAt: 1}); err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if err := s.DeleteExpireIndex(ctx, "n1"); err != nil {
		t.Fatalf("DeleteExpireIndex error: %v", err)
	}

	// record with expire: should delete the index entry
	if err := s.Put(ctx, &PinRecord{Cid: "e1", Status: StatusActive, LastUpdateAt: 1, ExpireAt: 999}); err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if err := s.DeleteExpireIndex(ctx, "e1"); err != nil {
		t.Fatalf("DeleteExpireIndex error: %v", err)
	}

	// Verify the expire index key is gone by attempting to iterate before ts
	cids, err := s.IndexByExpireBefore(ctx, 2000, 10)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
	}
	for _, c := range cids {
		if c == "e1" {
			t.Fatalf("expire index for e1 should have been deleted")
		}
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
		{Cid: "cid-m", Status: StatusActive, LastUpdateAt: 100}, // ts=100
		{Cid: "cid-a", Status: StatusActive, LastUpdateAt: 100}, // same ts, cid tie-breaker
		{Cid: "cid-z", Status: StatusActive, LastUpdateAt: 90},  // smallest ts
		{Cid: "cid-b", Status: StatusActive, LastUpdateAt: 110}, // largest ts
		{Cid: "skip-other-status", Status: StatusReceived, LastUpdateAt: 50},
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

	// Expected order: by ts asc, then by cid asc for equal ts
	expected := []string{"cid-z", "cid-a", "cid-m", "cid-b"}
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
		{Cid: "c-b", Status: StatusActive, LastUpdateAt: 1, ExpireAt: 200},
		{Cid: "c-z", Status: StatusActive, LastUpdateAt: 2, ExpireAt: 100},
		{Cid: "c-a", Status: StatusActive, LastUpdateAt: 3, ExpireAt: 100},
		{Cid: "c-c", Status: StatusActive, LastUpdateAt: 4, ExpireAt: 300},
	}
	for i := range items {
		rec := items[i]
		if err := s.Put(ctx, &rec); err != nil {
			t.Fatalf("Put error: %v", err)
		}
	}

	// Without tight upper bound to include all
	got, err := s.IndexByExpireBefore(ctx, 1000, 10)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
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
	got, err = s.IndexByExpireBefore(ctx, 1000, 2)
	if err != nil {
		t.Fatalf("IndexByExpireBefore error: %v", err)
	}
	expectedLimited := []string{"c-a", "c-z"}
	if len(got) != len(expectedLimited) || got[0] != expectedLimited[0] || got[1] != expectedLimited[1] {
		t.Fatalf("unexpected limited order: got %v want %v", got, expectedLimited)
	}
}
