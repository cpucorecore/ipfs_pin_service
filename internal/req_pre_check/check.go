package req_pre_check

import (
	"encoding/json"
	"errors"
	"github.com/cpucorecore/ipfs_pin_service/internal/util"
	"github.com/gin-gonic/gin"
	"strconv"
)

var (
	ErrEmptyCid      = errors.New("empty cid")
	ErrInvalidCid    = errors.New("invalid cid")
	ErrNegativeSize  = errors.New("size is negative")
	ErrInvalidPinMsg = errors.New("invalid pin message")
)

func CheckReq(cid string, size int64) (error, string, int64) {
	if !util.CheckCid(cid) {
		return ErrInvalidCid, "", 0
	}

	if size < 0 {
		return ErrNegativeSize, "", 0
	}

	return nil, cid, size
}

type PinMsg struct {
	Cid  string `json:"cid"`
	Size int64  `json:"size"`
}

func CheckJsonReq(data []byte) (error, string, int64) {
	pinMsg := &PinMsg{}
	err := json.Unmarshal(data, pinMsg)
	if err != nil {
		return ErrInvalidPinMsg, "", 0
	}

	return CheckReq(pinMsg.Cid, pinMsg.Size)
}

func CheckHttpReq(c *gin.Context) (error, string, int64) {
	cid := c.Param("cid")
	if cid == "" {
		return ErrEmptyCid, "", 0
	}

	sizeStr := c.Query("size")
	if sizeStr == "" {
		return CheckReq(cid, 0)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return err, "", 0
	}

	return CheckReq(cid, size)
}
