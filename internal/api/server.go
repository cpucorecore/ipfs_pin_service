package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/cpucorecore/ipfs_pin_service/internal/model"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	store Store
	queue MessageQueue
}

type Store interface {
	Get(ctx context.Context, cid string) (*model.PinRecord, error)
	Put(ctx context.Context, rec *model.PinRecord) error
	Update(ctx context.Context, cid string, apply func(*model.PinRecord) error) error
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
		rec = model.NewPinRecord(cid)
		// 如果提供了 size，直接设置
		if size > 0 {
			rec.SizeBytes = size
		}
		if err := s.store.Put(c, rec); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 入队
		body, err := proto.Marshal(rec)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, rec.ToView(model.TimeStyleISO))
		return
	}

	// 已存在的记录
	switch model.Status(rec.Status) {
	case model.StatusActive:
		// 刷新 TTL（通过 worker 处理）
		body, err := proto.Marshal(rec)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, rec.ToView(model.TimeStyleISO))

	case model.StatusQueuedForPin, model.StatusPinning:
		// 已在队列或处理中，直接返回
		c.JSON(http.StatusAccepted, rec.ToView(model.TimeStyleISO))

	default:
		// 其他状态，重新入队
		body, err := proto.Marshal(rec)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, rec.ToView(model.TimeStyleISO))
	}
}

func (s *Server) handleGetPin(c *gin.Context) {
	cid := c.Param("cid")
	if cid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing cid"})
		return
	}

	// 获取时间格式参数
	timeStyle := c.DefaultQuery("time_style", string(model.TimeStyleISO))
	var style model.TimeStyle
	switch timeStyle {
	case string(model.TimeStyleUnix):
		style = model.TimeStyleUnix
	case string(model.TimeStyleHuman):
		style = model.TimeStyleHuman
	default:
		style = model.TimeStyleISO
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

	c.JSON(http.StatusOK, rec.ToView(style))
}
