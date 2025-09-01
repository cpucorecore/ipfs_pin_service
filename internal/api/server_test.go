package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/filter"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
)

func TestMain(m *testing.M) {
	log.InitLoggerForTest()
	m.Run()
}

type fakeStore struct{ m map[string]*store.PinRecord }

func (f *fakeStore) Get(ctx context.Context, cid string) (*store.PinRecord, error) {
	if f.m == nil {
		f.m = map[string]*store.PinRecord{}
	}
	return f.m[cid], nil
}
func (f *fakeStore) Put(ctx context.Context, rec *store.PinRecord) error {
	if f.m == nil {
		f.m = map[string]*store.PinRecord{}
	}
	f.m[rec.Cid] = rec
	return nil
}
func (f *fakeStore) Update(ctx context.Context, cid string, apply func(*store.PinRecord) error) error {
	return nil
}
func (f *fakeStore) AddExpireIndex(ctx context.Context, cid string, expireAt int64) error {
	return nil
}
func (f *fakeStore) DeleteExpireIndex(ctx context.Context, cid string, expireAt int64) error {
	return nil
}
func (f *fakeStore) GetExpireCids(ctx context.Context, ts int64, limit int) ([]string, error) {
	return nil, nil
}
func (f *fakeStore) GetExpireIndex(ctx context.Context, cid string) (string, error) {
	return "", nil
}
func (f *fakeStore) Close() error {
	return nil
}

type fakeMQ struct{ payloads [][]byte }

func (q *fakeMQ) Enqueue(ctx context.Context, topic string, body []byte) error {
	q.payloads = append(q.payloads, body)
	return nil
}

func TestHandlePutPin_Filtered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fs := &fakeStore{}
	mq := &fakeMQ{}
	cfg := &config.Config{}
	cfg.Filter.SizeLimit = config.FileSize(1) // sizes >1 will be filtered
	f := filter.New(cfg)
	s := NewServer(fs, mq, f)
	r := gin.Default()
	s.Routes(r)

	req := httptest.NewRequest(http.MethodPut, "/pins/bafybeihtsfujfh73od4mr47jt24we7b6e77xx4c45ozegyjjvprltzhobi?size=2", bytes.NewBuffer(nil))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on filtered, got %d", w.Code)
	}
	rec := fs.m["bafybeihtsfujfh73od4mr47jt24we7b6e77xx4c45ozegyjjvprltzhobi"]
	if rec == nil || rec.Status != store.StatusFiltered {
		t.Fatalf("expected record filtered")
	}
	if rec.SizeLimit != int64(cfg.Filter.SizeLimit) {
		t.Fatalf("expected size limit recorded")
	}
}

func TestHandlePutPin_NonFiltered_Enqueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fs := &fakeStore{}
	mq := &fakeMQ{}
	s := NewServer(fs, mq, &filter.Filter{})
	r := gin.Default()
	s.Routes(r)

	req := httptest.NewRequest(http.MethodPut, "/pins/bafybeihtsfujfh73od4mr47jt24we7b6e77xx4c45ozegyjjvprltzhobi?size=0", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if len(mq.payloads) == 0 {
		t.Fatalf("expected enqueue payload")
	}
	var payload map[string]any
	_ = json.Unmarshal(mq.payloads[0], &payload)
	if payload["cid"].(string) != "bafybeihtsfujfh73od4mr47jt24we7b6e77xx4c45ozegyjjvprltzhobi" {
		t.Fatalf("unexpected payload")
	}

	// 验证状态更新
	rec := fs.m["bafybeihtsfujfh73od4mr47jt24we7b6e77xx4c45ozegyjjvprltzhobi"]
	if rec == nil {
		t.Fatalf("expected record to be stored")
	}
	if rec.Status != store.StatusEnqueued {
		t.Fatalf("expected status %d, got %d", store.StatusEnqueued, rec.Status)
	}
	if rec.EnqueuedAt == 0 {
		t.Fatalf("expected EnqueuedAt to be set")
	}
	if rec.ReceivedAt == 0 {
		t.Fatalf("expected ReceivedAt to be set")
	}
}
