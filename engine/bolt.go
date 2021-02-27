package engine

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

type implBolt struct {
	db      *bolt.DB
	buckets *sync.Map
}

type composedBoltKey []byte

func (k composedBoltKey) parseTs() time.Time {
	i := bytes.IndexByte(k, byte('.'))
	ts := k[:i]
	millis, err := strconv.ParseInt(string(ts), 10, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid key", string(k))
	}
	return time.Unix(millis/1000, millis%1000)
}

func (e *implBolt) ensureBucket(name []byte, tx *bolt.Tx) (*bbolt.Bucket, error) {
	k := string(name)
	_, ok := e.buckets.Load(k)
	if ok {
		return tx.Bucket(name), nil
	}
	b, err := tx.CreateBucketIfNotExists(name)
	if err == nil {
		e.buckets.Store(k, struct{}{})
	}
	return b, err
}

// Push an element to specified queue with given delay duration
func (e *implBolt) Push(name []byte, element []byte, delay time.Duration) error {
	return e.db.Update(func(t *bolt.Tx) error {
		b, err := e.ensureBucket(name, t)
		if err != nil {
			return err
		}
		tsMillis := time.Now().Add(delay).UnixNano() / int64(time.Millisecond)
		ts := []byte(strconv.FormatInt(tsMillis, 10))
		id := genID()
		segs := [][]byte{ts, id}
		key := bytes.Join(segs, []byte{byte('.')})
		return b.Put(key, element)
	})
}

func (e *implBolt) Pop(name []byte, limit int) ([][]byte, error) {
	results := make([][]byte, 0, limit)
	if limit <= 0 {
		return results, nil
	}
	now := time.Now()
	err := e.db.Update(func(t *bolt.Tx) error {
		b, err := e.ensureBucket(name, t)
		if err != nil {
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tm := composedBoltKey(k).parseTs()
			if tm.After(now) {
				break
			}
			results = append(results, v)
			if e := c.Delete(); e != nil {
				return e
			}
			limit--
			if limit <= 0 {
				break
			}
		}
		return nil
	})
	return results, err
}

// Get length of a queue
func (e *implBolt) Len(name []byte) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (e *implBolt) Queues() ([][]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (e *implBolt) Close() error {
	return e.db.Close()
}

func (e *implBolt) Del(name []byte) (int, error) {
	panic("not implemented") // TODO: Implement
}

// NewWithBolt creates a new delayed queue engine based on boltdb
func NewWithBolt(file string, opts *bolt.Options) (Interface, error) {
	db, err := bolt.Open(file, 0600, opts)
	if err != nil {
		return nil, err
	}
	return &implBolt{db: db, buckets: &sync.Map{}}, nil
}
