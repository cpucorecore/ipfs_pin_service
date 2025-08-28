package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/filter"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/internal/view_model"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
)

type Server struct {
	store  Store
	queue  MessageQueue
	filter *filter.Filter
}

type Store interface {
	Get(ctx context.Context, cid string) (*store.PinRecord, error)
	Put(ctx context.Context, rec *store.PinRecord) error
	Update(ctx context.Context, cid string, apply func(*store.PinRecord) error) error
}

type MessageQueue interface {
	Enqueue(ctx context.Context, topic string, body []byte) error
}

func NewServer(store Store, queue MessageQueue, f *filter.Filter) *Server {
	return &Server{
		store:  store,
		queue:  queue,
		filter: f,
	}
}

func (s *Server) Routes(r *gin.Engine) {
	r.PUT("/pins/:cid", s.handlePutPin)
	r.GET("/pins/:cid", s.handleGetPin)
}

func (s *Server) handlePutPin(c *gin.Context) {
	cidStr := c.Param("cid")
	if cidStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing cid"})
		return
	}

	if !util.CheckCid(cidStr) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cid"})
	}

	sizeStr := c.Query("size")
	var size int64
	if sizeStr != "" {
		var err error
		size, err = strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid size parameter"})
			return
		}
	}

	rec, err := s.store.Get(c, cidStr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	now := time.Now().UnixMilli()
	if rec == nil {
		rec = &store.PinRecord{
			Cid:          cidStr,
			Status:       store.StatusReceived,
			ReceivedAt:   now,
			LastUpdateAt: now,
		}
		if size > 0 {
			rec.Size = size
		}
	} else {
		// 处理已存在的记录
		switch rec.Status {
		case store.StatusUnpinSucceeded, store.StatusDeadLetter:
			// 已完成完整生命周期的记录，将当前状态存入历史，重置为首次请求状态
			historyRecord := &store.PinRecord{
				Cid:               rec.Cid,
				Status:            rec.Status,
				ReceivedAt:        rec.ReceivedAt,
				EnqueuedAt:        rec.EnqueuedAt,
				PinStartAt:        rec.PinStartAt,
				PinSucceededAt:    rec.PinSucceededAt,
				ExpireAt:          rec.ExpireAt,
				ScheduleUnpinAt:   rec.ScheduleUnpinAt,
				UnpinStartAt:      rec.UnpinStartAt,
				UnpinSucceededAt:  rec.UnpinSucceededAt,
				LastUpdateAt:      rec.LastUpdateAt,
				Size:              rec.Size,
				PinAttemptCount:   rec.PinAttemptCount,
				UnpinAttemptCount: rec.UnpinAttemptCount,
				SizeLimit:         rec.SizeLimit,
				History:           rec.History, // 保留原有历史
			}

			// 重置为首次请求状态
			rec.Status = store.StatusReceived
			rec.ReceivedAt = now
			rec.EnqueuedAt = 0
			rec.PinStartAt = 0
			rec.PinSucceededAt = 0
			rec.ExpireAt = 0
			rec.ScheduleUnpinAt = 0
			rec.UnpinStartAt = 0
			rec.UnpinSucceededAt = 0
			rec.LastUpdateAt = now
			rec.PinAttemptCount = 0
			rec.UnpinAttemptCount = 0
			rec.SizeLimit = 0

			// 将历史记录添加到历史数组
			if rec.History == nil {
				rec.History = []*store.PinRecord{}
			}
			rec.History = append(rec.History, historyRecord)

			// 更新大小（如果提供了新的大小）
			if size > 0 {
				rec.Size = size
			}

		case store.StatusFiltered:
			// 过滤状态，直接记录日志并跳过
			log.Log.Sugar().Infof("CID %s is filtered, skipping renewal", cidStr)
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
			return

		default:
			// 其他状态（处理中），直接返回当前状态，不做任何处理
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
			return
		}
	}

	if s.filter.ShouldFilter(size) {
		rec.Status = store.StatusFiltered
		rec.SizeLimit = s.filter.SizeLimit()
		rec.LastUpdateAt = now

		if err = s.store.Put(c, rec); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	body, _ := json.Marshal(gin.H{"cid": cidStr, "size": rec.Size})
	if err = s.queue.Enqueue(c, "pin.exchange", body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err = s.store.Put(c, rec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
}

func (s *Server) handleGetPin(c *gin.Context) {
	cidStr := c.Param("cid")
	if cidStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing cid"})
		return
	}

	if !util.CheckCid(cidStr) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cid"})
	}

	timeFormat := c.DefaultQuery("time_format", string(view_model.TimeFormatISO))
	var format view_model.TimeFormat
	switch timeFormat {
	case string(view_model.TimeFormatUnix):
		format = view_model.TimeFormatUnix
	case string(view_model.TimeFormatHuman):
		format = view_model.TimeFormatHuman
	default:
		format = view_model.TimeFormatISO
	}

	rec, err := s.store.Get(c, cidStr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if rec == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "record not found"})
		return
	}

	c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, format))
}
