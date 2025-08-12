package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

func makePinRecordKey(cid string) []byte {
	return []byte(fmt.Sprintf("%s/%s", prefixPinRecord, cid))
}

func makeStatusPrefix(status Status) []byte {
	return []byte(fmt.Sprintf("%s/%02d/", prefixStatus, status))
}

func makeStatusKey(status Status, ts int64, cid string) []byte {
	var buf bytes.Buffer
	buf.Write(makeStatusPrefix(status))
	binary.Write(&buf, binary.BigEndian, ts)
	buf.WriteString("/" + cid)
	return buf.Bytes()
}

func makeExpireKey(ts int64, cid string) []byte {
	var buf bytes.Buffer
	buf.WriteString(prefixExpire + "/")
	binary.Write(&buf, binary.BigEndian, ts)
	buf.WriteString("/" + cid)
	return buf.Bytes()
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

	if string(parts[0]) != prefixExpire {
		return 0, "", ErrWrongExpirePrefix
	}

	ts = int64(binary.BigEndian.Uint64(parts[1]))
	cid = string(parts[2])
	return
}
