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

	pinRecord, err := s.store.Get(c, cidStr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	now := time.Now().UnixMilli()
	if pinRecord == nil {
		pinRecord = &store.PinRecord{
			Cid:        cidStr,
			Status:     store.StatusReceived,
			ReceivedAt: now,
		}
		if size > 0 {
			pinRecord.Size = size
		}
	} else {
		switch pinRecord.Status {
		case store.StatusUnpinSucceeded, store.StatusDeadLetter:
			lastPinRecord, history := store.ClonePinRecord(pinRecord)
			store.ResetPinRecordDynamicState(pinRecord, 1+len(history))
			pinRecord.Status = store.StatusReceived
			pinRecord.ReceivedAt = now
			store.AppendHistory(pinRecord, lastPinRecord, history)

			if size > 0 {
				pinRecord.Size = size
			}

		case store.StatusFiltered:
			log.Log.Sugar().Infof("CID %s is filtered, skipping renewal", cidStr)
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
			return

		default:
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
			return
		}
	}

	if s.filter.ShouldFilter(size) {
		pinRecord.Status = store.StatusFiltered
		pinRecord.SizeLimit = s.filter.SizeLimit()
		if err = s.store.Put(c, pinRecord); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
		return
	}

	body, _ := json.Marshal(gin.H{"cid": cidStr, "size": pinRecord.Size})
	if err = s.queue.Enqueue(c, "pin.exchange", body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	pinRecord.Status = store.StatusEnqueued
	pinRecord.EnqueuedAt = time.Now().UnixMilli()
	if err = s.store.Put(c, pinRecord); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
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
