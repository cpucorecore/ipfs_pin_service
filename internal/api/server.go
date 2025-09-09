package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/filter"
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	reqprecheck "github.com/cpucorecore/ipfs_pin_service/internal/req_pre_check"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/cpucorecore/ipfs_pin_service/internal/view_model"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Server struct {
	store       store.Store
	queue       mq.Queue
	sizeFilter  *filter.SizeFilter
	shutdownMgr *shutdown.Manager
}

func NewServer(store store.Store, queue mq.Queue, sizeFilter *filter.SizeFilter, shutdownMgr *shutdown.Manager) *Server {
	return &Server{
		store:       store,
		queue:       queue,
		sizeFilter:  sizeFilter,
		shutdownMgr: shutdownMgr,
	}
}

func (s *Server) RegisterHandles(r *gin.Engine) {
	r.PUT("/pins/:cid", s.handlePutPin)
	r.GET("/pins/:cid", s.handleGetPin)
}

func (s *Server) handlePutPin(c *gin.Context) {
	if s.shutdownMgr.IsDraining() {
		log.Log.Warn("service is shutting down", zap.String("module", "api"))
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "service is shutting down, no longer accepting new requests",
		})
		return
	}

	err, cid, size := reqprecheck.CheckHttpReq(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pinRecord, err := s.store.Get(c, cid)
	if err != nil {
		log.Log.Error("get pin record err", zap.String("cid", cid), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	now := time.Now().UnixMilli()
	if pinRecord == nil {
		pinRecord = &store.PinRecord{
			Cid:        cid,
			Size:       size,
			Status:     store.StatusReceived,
			ReceivedAt: now,
		}
	} else {
		switch pinRecord.Status {
		case store.StatusUnpinSucceeded, store.StatusDeadLetter:
			lastPinRecord, history := store.ClonePinRecord(pinRecord)
			store.ResetPinRecordDynamicState(pinRecord, 1+len(history))
			pinRecord.Status = store.StatusReceived
			pinRecord.Size = size
			pinRecord.ReceivedAt = now
			store.AppendHistory(pinRecord, lastPinRecord, history)

		case store.StatusFiltered:
			log.Log.Info("CID is filtered, skipping renewal", zap.String("cid", cid))
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
			return

		default:
			c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
			return
		}
	}

	if s.sizeFilter.ShouldFilter(size) {
		pinRecord.Status = store.StatusFiltered
		pinRecord.SizeLimit = s.sizeFilter.SizeLimit()
		if err = s.store.Put(c, pinRecord); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, view_model.ConvertPinRecord(pinRecord, view_model.TimeFormatISO))
		return
	}

	body, _ := json.Marshal(gin.H{"cid": cid, "size": pinRecord.Size})
	err = s.queue.EnqueuePin(body)
	if err != nil {
		log.Log.Error("enqueue pin err", zap.String("msg", string(body)), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	pinRecord.Status = store.StatusEnqueued
	pinRecord.EnqueuedAt = time.Now().UnixMilli()
	if err = s.store.Put(c, pinRecord); err != nil {
		log.Log.Error("store.Put pinRecord err", zap.String("cid", cid), zap.Error(err))
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
		return
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
