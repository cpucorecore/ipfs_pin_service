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

	cloned, history := ClonePinRecord(original)

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

	// 验证返回的history切片
	if len(history) != 1 {
		t.Errorf("expected 1 history record, got %d", len(history))
	}
	if history[0].Cid != "history-1" {
		t.Errorf("expected history Cid 'history-1', got %s", history[0].Cid)
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

	ResetPinRecordDynamicState(rec, 2)

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

	// 验证History字段被重新初始化
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
	// 模拟实际场景：开始时没有历史记录
	rec := &PinRecord{Cid: "test-cid"}

	// 第一次：模拟记录从未被处理过，第一次被标记为UnpinSucceeded
	rec.Status = StatusUnpinSucceeded
	rec.ReceivedAt = 1000
	rec.LastUpdateAt = 2000

	// 第二次：模拟API场景 - 记录从UnpinSucceeded状态重新开始
	// 直接使用API中的组合：ClonePinRecord -> ResetPinRecordDynamicState -> AppendHistory
	lastPinRecord, history := ClonePinRecord(rec)
	ResetPinRecordDynamicState(rec, 1+len(history))
	rec.Status = StatusReceived
	rec.ReceivedAt = 3000
	AppendHistory(rec, lastPinRecord, history)

	if len(rec.History) != 1 {
		t.Errorf("expected 1 history record, got %d", len(rec.History))
	}
	// 验证历史记录按时间顺序排列：最旧的在前，最新的在后
	if rec.History[0].Status != StatusUnpinSucceeded {
		t.Errorf("expected history[0] Status %d, got %d", StatusUnpinSucceeded, rec.History[0].Status)
	}
	if rec.History[0].LastUpdateAt != 2000 {
		t.Errorf("expected history[0] LastUpdateAt 2000, got %d", rec.History[0].LastUpdateAt)
	}

	// 第三次：模拟API场景 - 记录从DeadLetter状态重新开始
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
	// 验证历史记录按时间顺序排列
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

	// 第四次：模拟API场景 - 记录再次重新开始
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
	// 验证历史记录按时间顺序排列
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

	// 测试添加多个历史记录
	for i := 0; i < 5; i++ {
		history := &PinRecord{
			Cid:    fmt.Sprintf("history-%d", i),
			Status: StatusUnpinSucceeded,
		}
		AppendHistory(rec, history, nil)
	}

	// 验证历史记录数量
	if len(rec.History) != 5 {
		t.Errorf("expected 5 history records, got %d", len(rec.History))
	}

	// 验证历史记录顺序（按时间顺序：最旧的在前，最新的在后）
	for i := 0; i < 5; i++ {
		expectedCid := fmt.Sprintf("history-%d", i)
		if rec.History[i].Cid != expectedCid {
			t.Errorf("expected history[%d] Cid '%s', got %s", i, expectedCid, rec.History[i].Cid)
		}
	}
}
