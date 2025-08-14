package util

import (
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/ipfs/go-cid"
)

func CheckCid(cidStr string) bool {
	_, err := cid.Decode(cidStr)
	if err != nil {
		log.Log.Sugar().Warnf("invalid cid %s", cidStr)
		return false
	}
	return true
}
