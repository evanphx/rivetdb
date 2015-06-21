package rivetdb

import (
	"crypto/sha1"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/evanphx/rivetdb/skiplist"
)

type DB struct {
	txid     int64
	readTxid *int64

	rwlock sync.Mutex

	skip     *skiplist.SkipList
	root     *Bucket
	nameLock sync.Mutex
}

type Tx struct {
	db       *DB
	txid     int64
	writable bool
}

func New() *DB {
	return &DB{
		readTxid: new(int64),
		skip:     skiplist.New(0),
	}
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	var txid int64

	if writable {
		db.rwlock.Lock()

		db.txid++
		txid = db.txid
	} else {
		txid = atomic.LoadInt64(db.readTxid)
	}

	return &Tx{db, txid, writable}, nil
}

func (tx *Tx) Id() int64 {
	return tx.txid
}

type Bucket struct {
	tx     *Tx
	prefix []byte
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	root := &Bucket{tx: tx}
	return root.Bucket(name)
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	h := sha1.New()
	h.Write(b.prefix)
	h.Write(name)

	sum := h.Sum(nil)

	key := append([]byte{'b'}, sum...)

	_, ok := b.tx.db.skip.Get(b.tx.txid, key)
	if !ok {
		return nil
	}

	return &Bucket{b.tx, []byte(sum)}
}

var ErrBucketExists = errors.New("bucket exists")

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	root := &Bucket{tx: tx}
	return root.CreateBucket(name)
}

func (b *Bucket) CreateBucket(name []byte) (*Bucket, error) {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	h := sha1.New()
	h.Write(b.prefix)
	h.Write(name)

	sum := h.Sum(nil)

	key := append([]byte{'b'}, sum...)

	_, ok := b.tx.db.skip.Get(b.tx.txid, key)
	if ok {
		return nil, ErrBucketExists
	}

	b.tx.db.skip.Set(b.tx.txid, key, name)

	return &Bucket{b.tx, []byte(sum)}, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	root := &Bucket{tx: tx}
	return root.CreateBucketIfNotExists(name)
}

func (b *Bucket) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	h := sha1.New()
	h.Write(b.prefix)
	h.Write(name)

	sum := h.Sum(nil)

	key := append([]byte{'b'}, sum...)

	_, ok := b.tx.db.skip.Get(b.tx.txid, key)
	if !ok {
		b.tx.db.skip.Set(b.tx.txid, key, name)
	}

	return &Bucket{b.tx, []byte(sum)}, nil
}

func (b *Bucket) vkey(key []byte) []byte {
	vkey := []byte{'v'}
	vkey = append(vkey, b.prefix...)
	vkey = append(vkey, key...)

	return vkey
}

func (b *Bucket) Put(key, val []byte) error {
	b.tx.db.skip.Set(b.tx.txid, b.vkey(key), val)
	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	val, ok := b.tx.db.skip.Get(b.tx.txid, b.vkey(key))
	if ok {
		return val
	}

	return nil
}

var ErrNotWritable = errors.New("tx not writable")

func (tx *Tx) Commit() error {
	if !tx.writable {
		return ErrNotWritable
	}

	atomic.StoreInt64(tx.db.readTxid, tx.txid)
	tx.db.rwlock.Unlock()

	return nil
}
