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
	Upsert(ctx context.Context, cid string, init func(*store.PinRecord), apply func(*store.PinRecord) error) (*store.PinRecord, bool, error)
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

	if s.filter.ShouldFilter(size) {
		rec, _, err := s.store.Upsert(c, cidStr,
			func(r *store.PinRecord) {
				now := time.Now().UnixMilli()
				r.Cid = cidStr
				r.ReceivedAt = now
				r.LastUpdateAt = now
				if size > 0 {
					r.SizeBytes = size
				}
			},
			func(r *store.PinRecord) error {
				r.Status = store.StatusFiltered
				r.FilterSizeLimit = s.filter.SizeLimit()
				r.LastUpdateAt = time.Now().UnixMilli()
				return nil
			},
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	rec, _, err := s.store.Upsert(c, cidStr,
		func(r *store.PinRecord) {
			now := time.Now().UnixMilli()
			r.Cid = cidStr
			r.Status = store.StatusReceived
			r.ReceivedAt = now
			r.LastUpdateAt = now
			if size > 0 {
				r.SizeBytes = size
			}
		},
		nil,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if rec.Status == store.StatusPinning {
		c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(rec, view_model.TimeFormatISO))
		return
	}

	body, _ := json.Marshal(gin.H{"cid": cidStr, "size": rec.SizeBytes})
	if err = s.queue.Enqueue(c, "pin.exchange", body); err != nil {
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
