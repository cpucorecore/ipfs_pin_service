package store

import (
	"bytes"
	"context"
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/cpucorecore/ipfs_pin_service/log"
	pb "github.com/cpucorecore/ipfs_pin_service/proto"
	"google.golang.org/protobuf/proto"
	"time"
)

const (
	prefixPinRecord = "p"
	prefixPinStatus = "s"
	prefixPinExpire = "e"
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

	pinRecord.LastUpdateAt = time.Now().UnixMilli()

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

var (
	ErrRecordNotFound = errors.New("record not found")
)

func (s *PebbleStore) Update(ctx context.Context, cid string, apply func(*PinRecord) error) error {
	pinRecord, err := s.Get(ctx, cid)
	if err != nil {
		return err
	}

	if pinRecord == nil {
		return ErrRecordNotFound
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

	pinRecord.LastUpdateAt = time.Now().UnixMilli()
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

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}

	return nil
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
	expireEndKey := makeExpireEndKey(ts)

	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: ExpireStartKey,
		UpperBound: expireEndKey,
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

func (s *PebbleStore) GetExpireIndex(ctx context.Context, cid string) (string, error) {
	rec, err := s.Get(ctx, cid)
	if err != nil {
		return "", err
	}
	if rec == nil || rec.ExpireAt == 0 {
		return "", nil
	}
	expireKey := makeExpireKey(rec.ExpireAt, cid)
	log.Log.Sugar().Debugf("get expire index key: %s", expireKey)
	_, _, err = s.db.Get(expireKey)
	return string(expireKey), err
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}

type pebbleIterator struct {
	iter    *pebble.Iterator
	started bool
}

func (i *pebbleIterator) Next() bool {
	if !i.started {
		i.started = true
		return i.iter.First()
	}
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
