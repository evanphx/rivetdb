package rivetdb

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/evanphx/rivetdb/skiplist"
	"github.com/evanphx/rivetdb/sstable"
)

type Stats struct {
	NumFlushes int
}

type DB struct {
	Path  string
	Stats Stats

	opts Options

	txid     int64
	readTxid *int64

	rwlock sync.Mutex

	skip     *skiplist.SkipList
	root     *Bucket
	nameLock sync.Mutex

	tables *sstable.Levels

	wal     *WAL
	walFile string

	memoryBytes int
	nextL0      int
	l0limit     int
}

type Tx struct {
	db       *DB
	txid     int64
	writable bool

	memoryBytes int
}

const DefaultMemoryBuffer = 1024 * 1024

type Options struct {
	MemoryBuffer int
	Debug        bool
}

func DefaultOptions() Options {
	return Options{
		MemoryBuffer: DefaultMemoryBuffer,
	}
}

func New(path string, opts Options) *DB {
	os.MkdirAll(path, 0755)

	buf := opts.MemoryBuffer
	if buf == 0 {
		buf = DefaultMemoryBuffer
	}

	db := &DB{
		Path:     path,
		opts:     opts,
		readTxid: new(int64),
		skip:     skiplist.New(0),
		tables:   sstable.NewLevels(10),
		walFile:  filepath.Join(path, "wal"),
		l0limit:  buf,
	}

	err := db.reloadWAL()
	if err != nil {
		panic(err)
	}

	wal, err := NewWAL(db.walFile)
	if err != nil {
		panic(err)
	}

	db.wal = wal

	return db
}

func (db *DB) reloadWAL() error {
	r, err := NewWALReader(db.walFile)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil
		}

		return err
	}

	list, err := r.IntoList()
	if err != nil {
		return err
	}

	db.txid = r.MaxCommittedVersion
	*db.readTxid = r.MaxCommittedVersion
	db.skip = list

	return nil
}

func (db *DB) Close() error {
	err := db.flushMemory()
	if err != nil {
		return err
	}

	return db.wal.Close()
}

func (db *DB) debug(str string, args ...interface{}) {
	if !db.opts.Debug {
		return
	}

	fmt.Fprintf(os.Stderr, str+"\n", args...)
}

func (db *DB) flushMemory() error {
	id := db.nextL0
	db.nextL0++

	path := filepath.Join(db.Path, fmt.Sprintf("level0_%d.sst", id))

	db.debug("Flushing memory values to: %s", path)

	db.Stats.NumFlushes++

	zw, err := NewZeroWriter(path, db.skip)
	if err != nil {
		return err
	}

	_, err = zw.Write()
	if err != nil {
		return err
	}

	err = db.tables.Add(0, path)
	if err != nil {
		return err
	}

	db.skip = skiplist.New(0)
	db.memoryBytes = 0
	return nil
}

func (db *DB) get(ver int64, key []byte) ([]byte, bool) {
	v, ok := db.skip.Get(ver, key)
	if ok {
		return v, true
	}

	v, err := db.tables.GetValue(ver, key)
	if err != nil {
		panic(err)
	}

	if v == nil {
		return nil, false
	}

	return v, true
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

	return &Tx{db: db, txid: txid, writable: writable}, nil
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

	_, ok := b.tx.db.get(b.tx.txid, key)
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

	_, ok := b.tx.db.get(b.tx.txid, key)
	if ok {
		return nil, ErrBucketExists
	}

	b.tx.db.skip.Set(b.tx.txid, key, name)

	err := b.tx.db.wal.Add(b.tx.txid, key, name)
	if err != nil {
		return nil, err
	}

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

	_, ok := b.tx.db.get(b.tx.txid, key)
	if !ok {
		b.tx.db.skip.Set(b.tx.txid, key, name)

		err := b.tx.db.wal.Add(b.tx.txid, key, name)
		if err != nil {
			return nil, err
		}
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
	b.tx.memoryBytes += sstable.EstimateMemory(key, val)

	vkey := b.vkey(key)

	b.tx.db.skip.Set(b.tx.txid, vkey, val)

	return b.tx.db.wal.Add(b.tx.txid, vkey, val)
}

func (b *Bucket) Get(key []byte) []byte {
	val, ok := b.tx.db.get(b.tx.txid, b.vkey(key))
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

	defer tx.db.rwlock.Unlock()

	err := tx.db.wal.Commit(tx.txid)
	if err != nil {
		return err
	}

	atomic.StoreInt64(tx.db.readTxid, tx.txid)

	tx.db.memoryBytes += tx.memoryBytes

	if tx.db.memoryBytes > tx.db.l0limit {
		err := tx.db.flushMemory()
		if err != nil {
			return err
		}

		err = tx.db.tables.ConsiderMerges(tx.db.Path, tx.txid)
		if err != nil {
			return err
		}
	}

	return nil
}
