package util

import (
	"github.com/ipfs/go-cid"
	"log"
)

func CheckCid(cidStr string) bool {
	_, err := cid.Decode(cidStr)
	if err != nil {
		log.Printf("invalid cid %s", cidStr)
		return false
	}
	return true
}
