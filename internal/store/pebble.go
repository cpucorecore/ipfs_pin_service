package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	pb "github.com/cpucorecore/ipfs_pin_service/proto"
	"google.golang.org/protobuf/proto"
)

const (
	prefixPinRecord = "p" // pin record
	prefixStatus    = "s" // status index
	prefixExpire    = "e" // expire index
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

func (s *PebbleStore) Get(ctx context.Context, cid string) (*PinRecord, error) {
	key := makePinRecordKey(cid)
	bytes, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer closer.Close()

	pinRecord := &pb.PinRecord{}
	if err = proto.Unmarshal(bytes, pinRecord); err != nil {
		return nil, err
	}

	return pinRecord, nil
}

func (s *PebbleStore) Put(ctx context.Context, pinRecord *PinRecord) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	bytes, err := proto.Marshal(pinRecord)
	if err != nil {
		return err
	}

	if err = batch.Set(makePinRecordKey(pinRecord.Cid), bytes, nil); err != nil {
		return err
	}

	if err = batch.Set(makeStatusKey(pinRecord.Status, pinRecord.LastUpdateAt, pinRecord.Cid), nil, nil); err != nil {
		return err
	}

	if pinRecord.ExpireAt > 0 {
		if err = batch.Set(makeExpireKey(pinRecord.ExpireAt, pinRecord.Cid), nil, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *PebbleStore) Update(ctx context.Context, cid string, apply func(*PinRecord) error) error {
	pinRecord, err := s.Get(ctx, cid)
	if err != nil {
		return err
	}

	if pinRecord == nil {
		return fmt.Errorf("record not found: %s", cid)
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err = batch.Delete(makeStatusKey(pinRecord.Status, pinRecord.LastUpdateAt, cid), nil); err != nil {
		return err
	}

	if pinRecord.ExpireAt > 0 {
		if err = batch.Delete(makeExpireKey(pinRecord.ExpireAt, cid), nil); err != nil {
			return err
		}
	}

	if err = apply(pinRecord); err != nil {
		return err
	}

	bytes, err := proto.Marshal(pinRecord)
	if err != nil {
		return err
	}

	if err = batch.Set(makePinRecordKey(cid), bytes, nil); err != nil {
		return err
	}

	if err = batch.Set(makeStatusKey(pinRecord.Status, pinRecord.LastUpdateAt, cid), nil, nil); err != nil {
		return err
	}

	if pinRecord.ExpireAt > 0 {
		if err = batch.Set(makeExpireKey(pinRecord.ExpireAt, cid), nil, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *PebbleStore) IndexByStatus(ctx context.Context, status Status) (Iterator[string], error) {
	prefix := makeStatusPrefix(status)
	upper := makeStatusPrefix(status + 1)
	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	return &pebbleIterator{iter: iter}, nil
}

func (s *PebbleStore) IndexByExpireBefore(ctx context.Context, ts int64, limit int) ([]string, error) {
	prefix := []byte(prefixExpire + "/")

	var upperBound bytes.Buffer
	upperBound.Write(prefix)
	binary.Write(&upperBound, binary.BigEndian, ts)

	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound.Bytes(),
	})
	defer iter.Close()

	var cids []string
	for iter.First(); iter.Valid() && len(cids) < limit; iter.Next() {
		key := iter.Key()
		_, cid, err := parseExpireKey(key)
		if err != nil {
			return nil, err
		}

		cids = append(cids, cid)
	}

	return cids, iter.Error()
}

func (s *PebbleStore) DeleteExpireIndex(ctx context.Context, cid string) error {
	rec, err := s.Get(ctx, cid)
	if err != nil {
		return err
	}
	if rec == nil || rec.ExpireAt == 0 {
		return nil
	}

	return s.db.Delete(makeExpireKey(rec.ExpireAt, cid), pebble.Sync)
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
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
