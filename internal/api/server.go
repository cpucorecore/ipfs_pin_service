package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/view_model"
	"github.com/gin-gonic/gin"
)

type Server struct {
	store Store
	queue MessageQueue
}

type Store interface {
	Get(ctx context.Context, cid string) (*store.PinRecord, error)
	Put(ctx context.Context, rec *store.PinRecord) error
	Update(ctx context.Context, cid string, apply func(*store.PinRecord) error) error
}

type MessageQueue interface {
	Enqueue(ctx context.Context, topic string, body []byte) error
}

func NewServer(store Store, queue MessageQueue) *Server {
	return &Server{
		store: store,
		queue: queue,
	}
}

func (s *Server) Routes(r *gin.Engine) {
	r.PUT("/pins/:cid", s.handlePutPin)
	r.GET("/pins/:cid", s.handleGetPin)
}

func (s *Server) handlePutPin(c *gin.Context) {
	cid := c.Param("cid")
	if cid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing cid"})
		return
	}

	// 获取可选的 size 参数
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

	rec, err := s.store.Get(c, cid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if rec == nil {
		// 新记录
		now := time.Now().UnixMilli()
		rec = &store.PinRecord{
			Cid:          cid,
			Status:       int32(store.StatusReceived),
			ReceivedAt:   now,
			LastUpdateAt: now,
		}
		// 如果提供了 size，直接设置
		if size > 0 {
			rec.SizeBytes = size
		}
		if err := s.store.Put(c, rec); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 入队（仅发送请求需要的信息）
		body, _ := json.Marshal(gin.H{"cid": cid, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	// 已存在的记录
	switch store.Status(rec.Status) {
	case store.StatusActive:
		// 刷新 TTL（通过 worker 处理）
		body, _ := json.Marshal(gin.H{"cid": cid, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))

	case store.StatusPinning:
		// 已在处理中，直接返回
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))

	default:
		// 其他状态，重新入队
		body, _ := json.Marshal(gin.H{"cid": cid, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
	}
}

func (s *Server) handleGetPin(c *gin.Context) {
	cid := c.Param("cid")
	if cid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing cid"})
		return
	}

	// 获取时间格式参数
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

	rec, err := s.store.Get(c, cid)
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
