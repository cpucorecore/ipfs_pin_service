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

	// Apply filter immediately after parsing size
	if s.filter != nil && s.filter.ShouldFilter(size) {
		now := time.Now().UnixMilli()
		if rec == nil {
			rec = &store.PinRecord{
				Cid:          cidStr,
				Status:       store.StatusFiltered,
				ReceivedAt:   now,
				LastUpdateAt: now,
			}
			if size > 0 {
				rec.SizeBytes = size
			}
			rec.FilterSizeLimit = s.filter.SizeLimit()
			if err = s.store.Put(c, rec); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else {
			now := time.Now().UnixMilli()
			if err := s.store.Update(c, cidStr, func(pr *store.PinRecord) error {
				pr.Status = store.StatusFiltered
				pr.FilterSizeLimit = s.filter.SizeLimit()
				if size > 0 {
					pr.SizeBytes = size
				}
				pr.LastUpdateAt = now
				return nil
			}); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			// reflect updates for response
			rec.Status = store.StatusFiltered
			rec.FilterSizeLimit = s.filter.SizeLimit()
			if size > 0 {
				rec.SizeBytes = size
			}
			rec.LastUpdateAt = now
		}

		c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	if rec == nil {
		now := time.Now().UnixMilli()
		rec = &store.PinRecord{
			Cid:          cidStr,
			Status:       store.StatusReceived,
			ReceivedAt:   now,
			LastUpdateAt: now,
		}
		// If client provided size, persist it
		if size > 0 {
			rec.SizeBytes = size
		}
		if err = s.store.Put(c, rec); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Enqueue minimal payload (cid, size)
		body, _ := json.Marshal(gin.H{"cid": cidStr, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	// Existing record handling
	switch rec.Status {
	case store.StatusActive:
		// Refresh TTL via worker
		body, _ := json.Marshal(gin.H{"cid": cidStr, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))

	case store.StatusPinning:
		// Already processing; return
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))

	default:
		// Requeue for processing
		body, _ := json.Marshal(gin.H{"cid": cidStr, "size": rec.SizeBytes})
		if err := s.queue.Enqueue(c, "pin.exchange", body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
	}
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

	// Parse time_format parameter
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
