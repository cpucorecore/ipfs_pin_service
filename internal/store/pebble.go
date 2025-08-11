package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cpucorecore/ipfs_pin_service/internal/model"
	pb "github.com/cpucorecore/ipfs_pin_service/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Key 前缀
	prefixRecord = "p" // pin record
	prefixStatus = "s" // status index
	prefixExpire = "e" // expire index
)

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db}, nil
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}

func (s *PebbleStore) Get(ctx context.Context, cid string) (*model.PinRecord, error) {
	key := makeRecordKey(cid)
	value, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	pbRec := &pb.PinRecord{}
	if err := proto.Unmarshal(value, pbRec); err != nil {
		return nil, err
	}
	return &model.PinRecord{PinRecord: pbRec}, nil
}

func (s *PebbleStore) Put(ctx context.Context, rec *model.PinRecord) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	// 序列化记录
	value, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	// 写主记录
	if err := batch.Set(makeRecordKey(rec.Cid), value, nil); err != nil {
		return err
	}

	// 写状态索引
	if err := batch.Set(makeStatusKey(model.Status(rec.Status), rec.LastUpdateAt, rec.Cid), nil, nil); err != nil {
		return err
	}

	// 写过期索引（如果有）
	if rec.ExpireAt > 0 {
		if err := batch.Set(makeExpireKey(rec.ExpireAt, rec.Cid), nil, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *PebbleStore) Update(ctx context.Context, cid string, apply func(*model.PinRecord) error) error {
	// 获取当前记录
	rec, err := s.Get(ctx, cid)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("record not found: %s", cid)
	}

	// 删除旧索引
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(makeStatusKey(model.Status(rec.Status), rec.LastUpdateAt, cid), nil); err != nil {
		return err
	}
	if rec.ExpireAt > 0 {
		if err := batch.Delete(makeExpireKey(rec.ExpireAt, cid), nil); err != nil {
			return err
		}
	}

	// 应用更新
	if err := apply(rec); err != nil {
		return err
	}

	// 序列化并写入新记录
	value, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	if err := batch.Set(makeRecordKey(cid), value, nil); err != nil {
		return err
	}

	// 写入新索引
	if err := batch.Set(makeStatusKey(model.Status(rec.Status), rec.LastUpdateAt, cid), nil, nil); err != nil {
		return err
	}
	if rec.ExpireAt > 0 {
		if err := batch.Set(makeExpireKey(rec.ExpireAt, cid), nil, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *PebbleStore) IndexByStatus(ctx context.Context, status model.Status) (Iterator[string], error) {
	prefix := []byte(fmt.Sprintf("%s/%d/", prefixStatus, status))
	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	return &pebbleIterator{iter: iter}, nil
}

func (s *PebbleStore) IndexByExpireBefore(ctx context.Context, ts int64, limit int) ([]string, error) {
	prefix := []byte(prefixExpire + "/")
	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, byte(ts>>56)),
	})
	defer iter.Close()

	var cids []string
	for iter.First(); iter.Valid() && len(cids) < limit; iter.Next() {
		key := iter.Key()
		_, _, cid, err := parseExpireKey(key)
		if err != nil {
			return nil, err
		}
		cids = append(cids, cid)
	}
	return cids, iter.Error()
}

// 辅助函数：构造键
func makeRecordKey(cid string) []byte {
	return []byte(fmt.Sprintf("%s/%s", prefixRecord, cid))
}

func makeStatusKey(status model.Status, ts int64, cid string) []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s/%d/", prefixStatus, status))
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

// 辅助函数：解析键
func parseExpireKey(key []byte) (prefix string, ts int64, cid string, err error) {
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 3 {
		return "", 0, "", fmt.Errorf("invalid expire key format")
	}

	prefix = string(parts[0])
	ts = int64(binary.BigEndian.Uint64(parts[1]))
	cid = string(parts[2])
	return
}

// Iterator 实现
type pebbleIterator struct {
	iter *pebble.Iterator
}

func (i *pebbleIterator) Next() bool {
	return i.iter.Next()
}

func (i *pebbleIterator) Value() string {
	key := i.iter.Key()
	parts := bytes.Split(key, []byte("/"))
	return string(parts[len(parts)-1])
}

func (i *pebbleIterator) Error() error {
	return i.iter.Error()
}

func (i *pebbleIterator) Close() error {
	return i.iter.Close()
}
