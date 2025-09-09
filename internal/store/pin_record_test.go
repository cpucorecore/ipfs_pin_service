package store

import (
	"fmt"
	"testing"
	"time"
)

func TestClonePinRecord(t *testing.T) {
	original := &PinRecord{
		Cid:                 "test-cid",
		Status:              StatusActive,
		ReceivedAt:          time.Now().UnixMilli(),
		Size:                1024,
		ProvideStartAt:      111,
		ProvideSucceededAt:  222,
		ProvideAttemptCount: 3,
		ProvideError:        "some error",
		History: []*PinRecord{
			{Cid: "history-1", Status: StatusUnpinSucceeded},
		},
	}

	cloned, history := ClonePinRecord(original)

	// Verify basic fields copied
	if cloned.Cid != original.Cid {
		t.Errorf("expected Cid %s, got %s", original.Cid, cloned.Cid)
	}
	if cloned.Status != original.Status {
		t.Errorf("expected Status %d, got %d", original.Status, cloned.Status)
	}
	if cloned.Size != original.Size {
		t.Errorf("expected Size %d, got %d", original.Size, cloned.Size)
	}

	// Verify provide-related fields copied
	if cloned.ProvideStartAt != original.ProvideStartAt {
		t.Errorf("expected ProvideStartAt %d, got %d", original.ProvideStartAt, cloned.ProvideStartAt)
	}
	if cloned.ProvideSucceededAt != original.ProvideSucceededAt {
		t.Errorf("expected ProvideSucceededAt %d, got %d", original.ProvideSucceededAt, cloned.ProvideSucceededAt)
	}
	if cloned.ProvideAttemptCount != original.ProvideAttemptCount {
		t.Errorf("expected ProvideAttemptCount %d, got %d", original.ProvideAttemptCount, cloned.ProvideAttemptCount)
	}
	if cloned.ProvideError != original.ProvideError {
		t.Errorf("expected ProvideError %q, got %q", original.ProvideError, cloned.ProvideError)
	}

	// Verify History not copied (avoid recursion)
	if cloned.History != nil {
		t.Errorf("expected History to be nil, got %v", cloned.History)
	}

	// Verify returned history slice
	if len(history) != 1 {
		t.Errorf("expected 1 history record, got %d", len(history))
	}
	if history[0].Cid != "history-1" {
		t.Errorf("expected history Cid 'history-1', got %s", history[0].Cid)
	}
}

func TestResetPinRecordDynamicState(t *testing.T) {
	rec := &PinRecord{
		Cid:                 "test-cid",
		Status:              StatusActive,
		ReceivedAt:          time.Now().UnixMilli(),
		Size:                1024,
		ProvideStartAt:      111,
		ProvideSucceededAt:  222,
		ProvideAttemptCount: 3,
		ProvideError:        "some error",
		History: []*PinRecord{
			{Cid: "history-1", Status: StatusUnpinSucceeded},
		},
	}

	ResetPinRecordDynamicState(rec, 2)

	// Verify dynamic fields reset
	if rec.Status != StatusUnknown {
		t.Errorf("expected Status %d, got %d", StatusUnknown, rec.Status)
	}
	if rec.ReceivedAt != 0 {
		t.Errorf("expected ReceivedAt 0, got %d", rec.ReceivedAt)
	}
	if rec.Size != 1024 {
		t.Errorf("expected Size to remain 1024, got %d", rec.Size)
	}

	// Verify provide-related fields cleared
	if rec.ProvideStartAt != 0 {
		t.Errorf("expected ProvideStartAt 0, got %d", rec.ProvideStartAt)
	}
	if rec.ProvideSucceededAt != 0 {
		t.Errorf("expected ProvideSucceededAt 0, got %d", rec.ProvideSucceededAt)
	}
	if rec.ProvideAttemptCount != 0 {
		t.Errorf("expected ProvideAttemptCount 0, got %d", rec.ProvideAttemptCount)
	}
	if rec.ProvideError != "" {
		t.Errorf("expected ProvideError empty, got %q", rec.ProvideError)
	}

	// Verify History reinitialized
	if rec.History == nil {
		t.Errorf("expected History to be initialized, got nil")
	}
	if cap(rec.History) != 2 {
		t.Errorf("expected History capacity 2, got %d", cap(rec.History))
	}
	if len(rec.History) != 0 {
		t.Errorf("expected History length 0, got %d", len(rec.History))
	}
}

func TestAppendHistory(t *testing.T) {
	// Simulate: no history initially
	rec := &PinRecord{Cid: "test-cid"}

	// First: record marked UnpinSucceeded for the first time
	rec.Status = StatusUnpinSucceeded
	rec.ReceivedAt = 1000
	rec.LastUpdateAt = 2000

	// Second: API flow restart from UnpinSucceeded
	// Use combination: ClonePinRecord -> ResetPinRecordDynamicState -> AppendHistory
	lastPinRecord, history := ClonePinRecord(rec)
	ResetPinRecordDynamicState(rec, 1+len(history))
	rec.Status = StatusReceived
	rec.ReceivedAt = 3000
	AppendHistory(rec, lastPinRecord, history)

	if len(rec.History) != 1 {
		t.Errorf("expected 1 history record, got %d", len(rec.History))
	}
	// Verify history in chronological order (oldest first)
	if rec.History[0].Status != StatusUnpinSucceeded {
		t.Errorf("expected history[0] Status %d, got %d", StatusUnpinSucceeded, rec.History[0].Status)
	}
	if rec.History[0].LastUpdateAt != 2000 {
		t.Errorf("expected history[0] LastUpdateAt 2000, got %d", rec.History[0].LastUpdateAt)
	}

	// Third: restart from DeadLetter
	rec.Status = StatusDeadLetter
	rec.ReceivedAt = 4000
	rec.LastUpdateAt = 5000

	lastPinRecord, history = ClonePinRecord(rec)
	ResetPinRecordDynamicState(rec, 1+len(history))
	rec.Status = StatusReceived
	rec.ReceivedAt = 6000
	AppendHistory(rec, lastPinRecord, history)

	if len(rec.History) != 2 {
		t.Errorf("expected 2 history records, got %d", len(rec.History))
	}
	// Verify order
	if rec.History[0].Status != StatusUnpinSucceeded {
		t.Errorf("expected history[0] Status %d, got %d", StatusUnpinSucceeded, rec.History[0].Status)
	}
	if rec.History[0].LastUpdateAt != 2000 {
		t.Errorf("expected history[0] LastUpdateAt 2000, got %d", rec.History[0].LastUpdateAt)
	}
	if rec.History[1].Status != StatusDeadLetter {
		t.Errorf("expected history[1] Status %d, got %d", StatusDeadLetter, rec.History[1].Status)
	}
	if rec.History[1].LastUpdateAt != 5000 {
		t.Errorf("expected history[1] LastUpdateAt 5000, got %d", rec.History[1].LastUpdateAt)
	}

	// Fourth: restart again
	rec.Status = StatusFiltered
	rec.ReceivedAt = 7000
	rec.LastUpdateAt = 8000

	lastPinRecord, history = ClonePinRecord(rec)
	ResetPinRecordDynamicState(rec, 1+len(history))
	rec.Status = StatusReceived
	rec.ReceivedAt = 9000
	AppendHistory(rec, lastPinRecord, history)

	if len(rec.History) != 3 {
		t.Errorf("expected 3 history records, got %d", len(rec.History))
	}
	// Verify order
	if rec.History[0].Status != StatusUnpinSucceeded {
		t.Errorf("expected history[0] Status %d, got %d", StatusUnpinSucceeded, rec.History[0].Status)
	}
	if rec.History[0].LastUpdateAt != 2000 {
		t.Errorf("expected history[0] LastUpdateAt 2000, got %d", rec.History[0].LastUpdateAt)
	}
	if rec.History[1].Status != StatusDeadLetter {
		t.Errorf("expected history[1] Status %d, got %d", StatusDeadLetter, rec.History[1].Status)
	}
	if rec.History[1].LastUpdateAt != 5000 {
		t.Errorf("expected history[1] LastUpdateAt 5000, got %d", rec.History[1].LastUpdateAt)
	}
	if rec.History[2].Status != StatusFiltered {
		t.Errorf("expected history[2] Status %d, got %d", StatusFiltered, rec.History[2].Status)
	}
	if rec.History[2].LastUpdateAt != 8000 {
		t.Errorf("expected history[2] LastUpdateAt 8000, got %d", rec.History[2].LastUpdateAt)
	}
}

func TestAppendHistoryWithMultipleRecords(t *testing.T) {
	rec := &PinRecord{Cid: "test-cid"}

	// Test append multiple history entries
	for i := 0; i < 5; i++ {
		history := &PinRecord{
			Cid:    fmt.Sprintf("history-%d", i),
			Status: StatusUnpinSucceeded,
		}
		AppendHistory(rec, history, nil)
	}

	// Verify count
	if len(rec.History) != 5 {
		t.Errorf("expected 5 history records, got %d", len(rec.History))
	}

	// Verify order (oldest first)
	for i := 0; i < 5; i++ {
		expectedCid := fmt.Sprintf("history-%d", i)
		if rec.History[i].Cid != expectedCid {
			t.Errorf("expected history[%d] Cid '%s', got %s", i, expectedCid, rec.History[i].Cid)
		}
	}
}
