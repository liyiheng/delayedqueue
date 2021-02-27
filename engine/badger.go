package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

var (
	queueNamePrefix = "queue_name."
	queueID         = []byte("queue_id")
)

type implBadger struct {
	db       *badger.DB
	gcTicker *time.Ticker
	seq      *badger.Sequence
	queues   *sync.Map
}

func (e *implBadger) getQueueID(name []byte) (uint64, error) {
	val, ok := e.queues.Load(string(name))
	if ok {
		return val.(uint64), nil
	}
	// TODO lock()
	// TODO check e.queues
	// TODO seq.next()
	// TODO defer unlock()
	id, err := e.seq.Next()
	if err != nil {
		return 0, err
	}
	idStr := strconv.FormatUint(id, 10)
	err = e.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(queueNamePrefix+idStr), name)
	})
	if err == nil {
		e.queues.Store(string(name), id)
	}
	return id, err
}

type composedKey []byte

func (k composedKey) parseTs(prefixSize int) time.Time {
	body := k[prefixSize:]
	i := bytes.IndexByte(body, byte('.'))
	body = body[:i]
	millis, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid key", string(k))
	}
	return time.Unix(millis/1000, millis%1000)
}

func genID() []byte {
	// TODO optimize, TiKV, nextKey
	id := make([]byte, 4)
	rand.Read(id)
	return id
}

// Push an element to specified queue with given delay duration
func (e *implBadger) Push(name []byte, element []byte, delay time.Duration) error {
	id, err := e.getQueueID(name)
	if err != nil {
		return nil
	}
	prefix := []byte(strconv.FormatUint(id, 36))
	return e.db.Update(func(txn *badger.Txn) error {
		tsMillis := time.Now().Add(delay).UnixNano() / int64(time.Millisecond)
		ts := []byte(strconv.FormatInt(tsMillis, 10)) // TODO 10 => 36
		id := genID()
		segs := [][]byte{prefix, ts, id}
		key := bytes.Join(segs, []byte{byte('.')})
		return txn.Set(key, element)
	})
}

func (e *implBadger) Pop(name []byte, limit int) ([][]byte, error) {
	results := make([][]byte, 0, limit)
	id, err := e.getQueueID(name)
	if err != nil {
		return nil, err
	}
	txn := e.db.NewTransaction(true)
	defer txn.Discard()
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	prefix := []byte(strconv.FormatUint(id, 36))
	prefix = append(prefix, byte('.'))
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := make([]byte, len(item.Key()))
		copy(k, item.Key())
		tm := composedKey(k).parseTs(len(prefix))
		if tm.After(time.Now()) {
			break
		}
		err := item.Value(func(val []byte) error {
			v := make([]byte, len(val))
			copy(v, val)
			results = append(results, v)
			limit--
			return txn.Delete(k)
		})
		if err != nil {
			return nil, err
		}

		if limit <= 0 {
			break
		}
	}
	it.Close()
	err = txn.Commit()
	return results, err
}

// Get length of a queue
func (e *implBadger) Len(name []byte) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (e *implBadger) Del(name []byte) (int, error) {
	panic("not implemented") // TODO: Implement
}

func (e *implBadger) Queues() ([][]byte, error) {
	names := make([][]byte, 0)
	e.queues.Range(func(key, value interface{}) bool {
		names = append(names, []byte(key.(string)))
		return true
	})
	return names, nil
}

func (e *implBadger) Close() error {
	e.gcTicker.Stop()
	e.seq.Release()
	return e.db.Close()
}

// NewWithBadger creates a new delayed queue engine based on badger
func NewWithBadger(opts badger.Options) (Interface, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	seq, err := db.GetSequence(queueID, 4)
	if err != nil {
		db.Close()
		return nil, err
	}
	// TODO Move GC ticker duration to configs
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
		again:
			// TODO Move discard ratio to configs
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}

	}()
	queues := &sync.Map{}
	b := &implBadger{db, ticker, seq, queues}
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(queueNamePrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			idVal := bytes.Split(item.Key(), []byte("."))[1]
			id, err := strconv.ParseUint(string(idVal), 36, 64)
			item.Value(func(val []byte) error {
				queues.Store(string(val), id)
				return nil
			})
			return err
		}
		return nil
	})
	if err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}
