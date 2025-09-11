package store

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
	pb "github.com/cpucorecore/ipfs_pin_service/proto"
	"google.golang.org/protobuf/proto"
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
	data, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer closer.Close()

	pinRecord := &pb.PinRecord{}
	if err = proto.Unmarshal(data, pinRecord); err != nil {
		return nil, err
	}

	return pinRecord, nil
}

func (s *PebbleStore) Put(ctx context.Context, pinRecord *PinRecord) error {
	pinRecord.LastUpdateAt = time.Now().UnixMilli()
	data, err := proto.Marshal(pinRecord)
	if err != nil {
		return err
	}

	return s.db.Set(makePinRecordKey(pinRecord.Cid), data, pebble.Sync)
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

	if err = apply(pinRecord); err != nil {
		return err
	}

	return s.Put(ctx, pinRecord)
}

func (s *PebbleStore) AddExpireIndex(ctx context.Context, cid string, expireAt int64) error {
	expireKey := makeExpireKey(expireAt, cid)
	return s.db.Set(expireKey, nil, pebble.Sync)
}

func (s *PebbleStore) DeleteExpireIndex(ctx context.Context, cid string, expireAt int64) error {
	expireKey := makeExpireKey(expireAt, cid)
	return s.db.Delete(expireKey, pebble.Sync)
}

func (s *PebbleStore) DeleteExpireIndexByKey(ctx context.Context, key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

func (s *PebbleStore) GetExpires(ctx context.Context, timestamp int64, limit int) ([]*Expire, error) {
	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: ExpireStartKey,
		UpperBound: makeExpireEndKey(timestamp),
	})
	defer iter.Close()

	expires := make([]*Expire, 0, limit)
	for iter.First(); iter.Valid() && len(expires) < limit; iter.Next() {
		key := iter.Key()
		_, cid, err := parseExpireKey(key)
		if err != nil {
			return nil, err
		}
		expires = append(expires, &Expire{
			Key: key,
			Cid: cid,
		})
	}
	return expires, iter.Error()
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}
