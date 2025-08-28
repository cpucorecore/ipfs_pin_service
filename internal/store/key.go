package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

var (
	ExpireStartKey    = []byte(prefixPinExpire + "/")
	ExpireStartKeyLen = 2
)

func makePinRecordKey(cid string) []byte {
	return []byte(fmt.Sprintf("%s/%s", prefixPinRecord, cid))
}

func makeStatusPrefix(status Status) []byte {
	return []byte(fmt.Sprintf("%s/%02d/", prefixPinStatus, status))
}

func makeStatusKey(status Status, ts int64, cid string) []byte {
	var buf bytes.Buffer
	buf.Write(makeStatusPrefix(status))
	binary.Write(&buf, binary.BigEndian, ts)
	buf.WriteString("/" + cid)
	return buf.Bytes()
}

func makeExpireEndKey(ts int64) []byte {
	buffer := make([]byte, 0, ExpireStartKeyLen+8)
	key := bytes.NewBuffer(buffer[:])
	key.Write(ExpireStartKey)
	key.WriteString(fmt.Sprintf("%d", ts))
	return key.Bytes()
}

func makeExpireKey(ts int64, cid string) []byte {
	buffer := make([]byte, 0, ExpireStartKeyLen+8+1+len(cid))
	key := bytes.NewBuffer(buffer[:])
	key.Write(ExpireStartKey)
	key.WriteString(fmt.Sprintf("%d", ts))
	key.WriteString("/" + cid)
	return key.Bytes()
}

var (
	ErrWrongExpirePrefix = errors.New("wrong expire prefix")
	ErrInvalidExpireKey  = errors.New("invalid expire key")
)

func parseExpireKey(key []byte) (ts int64, cid string, err error) {
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 3 {
		return 0, "", ErrInvalidExpireKey
	}

	if string(parts[0]) != prefixPinExpire {
		return 0, "", ErrWrongExpirePrefix
	}

	ts, err = strconv.ParseInt(string(parts[1]), 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse timestamp: %w", err)
	}

	cid = string(parts[2])
	return
}
