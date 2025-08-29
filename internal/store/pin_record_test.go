package store

import (
	"fmt"
	"testing"
	"time"
)

func TestClonePinRecord(t *testing.T) {
	original := &PinRecord{
		Cid:        "test-cid",
		Status:     StatusActive,
		ReceivedAt: time.Now().UnixMilli(),
		Size:       1024,
		History: []*PinRecord{
			{Cid: "history-1", Status: StatusUnpinSucceeded},
		},
	}

	cloned := ClonePinRecord(original)

	// 验证基本字段被正确复制
	if cloned.Cid != original.Cid {
		t.Errorf("expected Cid %s, got %s", original.Cid, cloned.Cid)
	}
	if cloned.Status != original.Status {
		t.Errorf("expected Status %d, got %d", original.Status, cloned.Status)
	}
	if cloned.Size != original.Size {
		t.Errorf("expected Size %d, got %d", original.Size, cloned.Size)
	}

	// 验证History字段没有被复制（避免递归）
	if cloned.History != nil {
		t.Errorf("expected History to be nil, got %v", cloned.History)
	}
}

func TestResetPinRecordDynamicState(t *testing.T) {
	rec := &PinRecord{
		Cid:        "test-cid",
		Status:     StatusActive,
		ReceivedAt: time.Now().UnixMilli(),
		Size:       1024,
		History: []*PinRecord{
			{Cid: "history-1", Status: StatusUnpinSucceeded},
		},
	}

	ResetPinRecordDynamicState(rec)

	// 验证动态状态被重置
	if rec.Status != StatusUnknown {
		t.Errorf("expected Status %d, got %d", StatusUnknown, rec.Status)
	}
	if rec.ReceivedAt != 0 {
		t.Errorf("expected ReceivedAt 0, got %d", rec.ReceivedAt)
	}
	if rec.Size != 1024 {
		t.Errorf("expected Size to remain 1024, got %d", rec.Size)
	}

	// 验证History字段被清理
	if rec.History != nil {
		t.Errorf("expected History to be nil, got %v", rec.History)
	}
}

func TestAppendHistoryPinRecord(t *testing.T) {
	rec := &PinRecord{Cid: "test-cid"}
	history1 := &PinRecord{Cid: "history-1", Status: StatusUnpinSucceeded}
	history2 := &PinRecord{Cid: "history-2", Status: StatusDeadLetter}

	// 测试添加第一个历史记录
	AppendHistory(rec, history1)
	if len(rec.History) != 1 {
		t.Errorf("expected 1 history record, got %d", len(rec.History))
	}
	if rec.History[0].Cid != "history-1" {
		t.Errorf("expected history Cid 'history-1', got %s", rec.History[0].Cid)
	}

	// 测试添加第二个历史记录
	AppendHistory(rec, history2)
	if len(rec.History) != 2 {
		t.Errorf("expected 2 history records, got %d", len(rec.History))
	}
	if rec.History[1].Cid != "history-2" {
		t.Errorf("expected history Cid 'history-2', got %s", rec.History[1].Cid)
	}
}

func TestAppendHistoryPinRecordDepthLimit(t *testing.T) {
	rec := &PinRecord{Cid: "test-cid"}

	// 添加超过最大深度的历史记录
	for i := 0; i < MaxHistoryDepth+5; i++ {
		history := &PinRecord{
			Cid:    fmt.Sprintf("history-%d", i),
			Status: StatusUnpinSucceeded,
		}
		AppendHistory(rec, history)
	}

	// 验证历史记录数量不超过最大深度
	if len(rec.History) != MaxHistoryDepth {
		t.Errorf("expected %d history records, got %d", MaxHistoryDepth, len(rec.History))
	}

	// 验证最旧的历史记录被移除（应该是最新的5个记录）
	expectedCids := []string{"history-5", "history-6", "history-7", "history-8", "history-9"}
	for i, expectedCid := range expectedCids {
		if rec.History[i].Cid != expectedCid {
			t.Errorf("expected history[%d] Cid '%s', got %s", i, expectedCid, rec.History[i].Cid)
		}
	}
}
