package rivetdb

import (
	"errors"
	"sync"
	"sync/atomic"
)

type namespace struct {
	ver int64
	mem *MemTable

	sub map[string]*namespace
}

type DB struct {
	txid     int64
	readTxid *int64

	rwlock sync.Mutex

	root     *namespace
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
		root: &namespace{
			mem: NewMemTable(),
			sub: make(map[string]*namespace),
		},
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
	tx *Tx
	ns *namespace
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	tx.db.nameLock.Lock()
	defer tx.db.nameLock.Unlock()

	ns, ok := tx.db.root.sub[string(name)]
	if !ok {
		return nil
	}

	if ns.ver <= tx.txid {
		return &Bucket{tx, ns}
	}

	return nil
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	ns, ok := b.ns.sub[string(name)]
	if !ok {
		return nil
	}

	if ns.ver <= b.tx.txid {
		return &Bucket{b.tx, ns}
	}

	return nil
}

var ErrBucketExists = errors.New("bucket exists")

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	tx.db.nameLock.Lock()
	defer tx.db.nameLock.Unlock()

	_, ok := tx.db.root.sub[string(name)]
	if ok {
		return nil, ErrBucketExists
	}

	ns := &namespace{
		ver: tx.txid,
		mem: NewMemTable(),
		sub: make(map[string]*namespace),
	}

	tx.db.root.sub[string(name)] = ns

	return &Bucket{tx, ns}, nil
}

func (b *Bucket) CreateBucket(name []byte) (*Bucket, error) {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	_, ok := b.ns.sub[string(name)]
	if ok {
		return nil, ErrBucketExists
	}

	ns := &namespace{
		ver: b.tx.txid,
		mem: NewMemTable(),
		sub: make(map[string]*namespace),
	}

	b.ns.sub[string(name)] = ns

	return &Bucket{b.tx, ns}, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	tx.db.nameLock.Lock()
	defer tx.db.nameLock.Unlock()

	ns, ok := tx.db.root.sub[string(name)]
	if !ok {
		ns = &namespace{
			ver: tx.txid,
			mem: NewMemTable(),
			sub: make(map[string]*namespace),
		}

		tx.db.root.sub[string(name)] = ns
	}

	return &Bucket{tx, ns}, nil
}

func (b *Bucket) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	b.tx.db.nameLock.Lock()
	defer b.tx.db.nameLock.Unlock()

	ns, ok := b.ns.sub[string(name)]
	if !ok {
		ns = &namespace{
			ver: b.tx.txid,
			mem: NewMemTable(),
			sub: make(map[string]*namespace),
		}

		b.ns.sub[string(name)] = ns
	}

	return &Bucket{b.tx, ns}, nil
}

func (b *Bucket) Put(key, val []byte) error {
	b.ns.mem.Put(b.tx.txid, key, val)
	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	val, ok := b.ns.mem.Get(b.tx.txid, key)
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
