package rivetdb

import (
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/evanphx/rivetdb/skiplist"
	"github.com/evanphx/rivetdb/sstable"
	"gopkg.in/tomb.v2"
)

// Indicates that the DB is currently locked by another process
var ErrDBLocked = errors.New("db locked")

// Stats holds statistics about the DB's operation
type Stats struct {
	NumFlushes int
}

type state struct {
	Txid   int64 `json:"txid"`
	FileId int   `json:"file_id"`

	Version *Version `json:"version"`

	versionLock sync.Mutex
}

func (s *state) LoadVersion() *Version {
	s.versionLock.Lock()

	ver := s.Version
	ver.Ref()

	s.versionLock.Unlock()

	return ver
}

func (s *state) SetVersion(t *Version) {
	s.versionLock.Lock()

	old := s.Version

	if t != old {
		t.Ref()

		s.Version = t
	} else if old != nil {
		old.Discard()
	}

	s.versionLock.Unlock()
}

// DB is a Rivetdb reflected by the state of a particular directory
type DB struct {
	Path  string
	Stats Stats

	opts Options

	state    state
	readTxid *int64

	rwlock sync.Mutex

	root     *Bucket
	nameLock sync.Mutex

	wal     *WAL
	walFile string

	lock     *os.File
	lockFile string

	memoryBytes int
	nextL0      int
	l0limit     int

	t          tomb.Tomb
	compactReq chan *Version

	taskCount int
	idleLock  sync.Mutex
	idleCond  *sync.Cond
}

// Tx is a database transaction
type Tx struct {
	db       *DB
	txid     int64
	writable bool

	memoryBytes int

	local   *skiplist.SkipList
	version *Version
}

// The default number of bytes to hold in memory before flushing it to disk
const DefaultMemoryBuffer = 1024 * 1024

// Options is the controlable values that can be set for a database on open
type Options struct {
	MemoryBuffer int
	Debug        bool
}

// New opens a database for the data in +path+
func New(path string, opts Options) (*DB, error) {
	os.MkdirAll(path, 0755)

	buf := opts.MemoryBuffer
	if buf == 0 {
		buf = DefaultMemoryBuffer
	}

	db := &DB{
		Path:       path,
		opts:       opts,
		readTxid:   new(int64),
		walFile:    filepath.Join(path, "wal"),
		lockFile:   filepath.Join(path, "lock"),
		l0limit:    buf,
		compactReq: make(chan *Version),
	}

	db.idleCond = sync.NewCond(&db.idleLock)

	db.state.SetVersion(&Version{
		Id:     0,
		Tables: sstable.NewLevels(10),
		mem:    skiplist.New(0),
	})

	flags := os.O_CREATE | os.O_RDWR

	lck, err := os.OpenFile(db.lockFile, flags, 0600)
	if err != nil {
		return nil, ErrDBLocked
	}

	err = syscall.Flock(int(lck.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, ErrDBLocked
	}

	db.lock = lck

	err = db.loadState()
	if err != nil {
		return nil, err
	}

	err = db.reloadWAL()
	if err != nil {
		return nil, err
	}

	wal, err := NewWAL(db.walFile)
	if err != nil {
		return nil, err
	}

	db.wal = wal

	db.t.Go(db.compact)

	return db, nil
}

func (db *DB) WaitIdle() {
	db.idleLock.Lock()
	defer db.idleLock.Unlock()

	for db.taskCount > 0 {
		db.idleCond.Wait()
	}
}

func (db *DB) finishTask() {
	db.idleLock.Lock()
	defer db.idleLock.Unlock()

	db.taskCount--

	if db.taskCount == 0 {
		db.idleCond.Broadcast()
	}
}

func (db *DB) startTask() {
	db.idleLock.Lock()
	defer db.idleLock.Unlock()

	db.taskCount++
}

func (db *DB) loadState() error {
	f, err := os.Open(filepath.Join(db.Path, "state"))
	if err != nil {
		return nil
	}

	defer f.Close()

	err = json.NewDecoder(f).Decode(&db.state)
	if err != nil {
		return err
	}

	*db.readTxid = db.state.Txid

	return nil
}

func (db *DB) saveState() error {
	f, err := os.Create(filepath.Join(db.Path, "state"))
	if err != nil {
		return nil
	}

	defer f.Close()

	return json.NewEncoder(f).Encode(db.state)
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

	db.state.Txid = r.MaxCommittedVersion
	*db.readTxid = r.MaxCommittedVersion

	db.state.Version.mem = list

	return nil
}

func (db *DB) unlock() error {
	return syscall.Flock(int(db.lock.Fd()), syscall.LOCK_UN)
}

func (db *DB) Close() error {
	err := db.flushMemory()
	if err != nil {
		return err
	}

	db.wal.Close()

	err = db.saveState()
	if err != nil {
		return err
	}

	return db.unlock()
}

func (db *DB) debug(str string, args ...interface{}) {
	if !db.opts.Debug {
		return
	}

	fmt.Fprintf(os.Stderr, str+"\n", args...)
}

func (db *DB) flushMemory() error {
	ver := db.state.LoadVersion()
	defer ver.Discard()

	update, err := db.flushVersion(ver)
	if err != nil {
		return err
	}

	defer update.Discard()

	db.state.SetVersion(update)

	db.memoryBytes = 0
	return nil
}

func (db *DB) flushVersion(ver *Version) (*Version, error) {
	id := db.state.FileId
	db.state.FileId++

	path := filepath.Join(db.Path, fmt.Sprintf("level0_%d.sst", id))

	db.debug("Flushing memory values to: %s", path)

	db.Stats.NumFlushes++

	var list *skiplist.SkipList

	if ver.imm != nil {
		list = ver.imm
	} else {
		list = ver.mem
	}

	zw, err := NewZeroWriter(path, list)
	if err != nil {
		return nil, err
	}

	_, err = zw.Write()
	if err != nil {
		return nil, err
	}

	edits := sstable.LevelsEdit{
		0: sstable.LevelEdit{
			Add: []string{path},
		},
	}

	levels, err := ver.Tables.Edit(edits)
	if err != nil {
		return nil, err
	}

	return NewVersion(db.state.Txid, levels), nil
}

func (tx *Tx) get(ver int64, key []byte) ([]byte, bool) {
	v, ok := tx.local.Get(ver, key)
	if ok {
		return v, true
	}

	if tx.version.imm != nil {
		v, ok := tx.version.imm.Get(ver, key)
		if ok {
			return v, true
		}
	}

	v, ok = tx.version.mem.Get(ver, key)
	if ok {
		return v, true
	}

	v, err := tx.version.Tables.GetValue(ver, key)
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

		db.state.Txid++
		txid = db.state.Txid
	} else {
		txid = atomic.LoadInt64(db.readTxid)
	}

	tx := &Tx{
		db:       db,
		txid:     txid,
		writable: writable,
		local:    skiplist.New(0),
		version:  db.state.LoadVersion(),
	}

	return tx, nil
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

	_, ok := b.tx.get(b.tx.txid, key)
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

	_, ok := b.tx.get(b.tx.txid, key)
	if ok {
		return nil, ErrBucketExists
	}

	b.tx.local.Set(b.tx.txid, key, name)

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

	_, ok := b.tx.get(b.tx.txid, key)
	if !ok {
		b.tx.local.Set(b.tx.txid, key, name)

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

	b.tx.local.Set(b.tx.txid, vkey, val)

	return b.tx.db.wal.Add(b.tx.txid, vkey, val)
}

func (b *Bucket) Get(key []byte) []byte {
	val, ok := b.tx.get(b.tx.txid, b.vkey(key))
	if ok {
		return val
	}

	return nil
}

var ErrNotWritable = errors.New("tx not writable")

func (db *DB) compact() error {
	for {
		select {
		case ver := <-db.compactReq:
			err := db.compactVersion(ver)
			if err != nil {
				log.Printf("Error compacting version: %s", err)
			}

			db.finishTask()

		case <-db.t.Dying():
			return nil
		}
	}

	return nil
}

func (db *DB) compactVersion(ver *Version) error {
	defer ver.Discard()

	zupdate, err := db.flushVersion(ver)
	if err != nil {
		return err
	}

	defer zupdate.Discard()

	updates, err := zupdate.Tables.ConsiderMerges(db.Path, ver.Id)
	if err != nil {
		return err
	}

	if zupdate.Tables != updates {
		db.state.SetVersion(zupdate.UpdateTables(updates))
		updates.Discard()
	} else {
		db.state.SetVersion(zupdate)
	}

	db.memoryBytes = 0

	return nil
}

func (db *DB) makeImmutable(ver *Version) *Version {
	imm := ver.MakeImmutable()

	db.state.SetVersion(imm)

	return imm
}

func (tx *Tx) injectLocal() {
	all := tx.local.AllEntries()

	for all.Next() {
		val, _ := all.Value(0)
		tx.version.mem.Set(tx.txid, all.Key(), val)
	}
}

func (tx *Tx) Commit() error {
	if !tx.writable {
		return ErrNotWritable
	}

	defer tx.db.rwlock.Unlock()

	err := tx.db.wal.Commit(tx.txid)
	if err != nil {
		return err
	}

	tx.injectLocal()

	defer tx.version.Discard()

	atomic.StoreInt64(tx.db.readTxid, tx.txid)

	tx.db.memoryBytes += tx.memoryBytes

	if tx.db.memoryBytes > tx.db.l0limit {
		tx.db.startTask()
		tx.db.compactReq <- tx.db.makeImmutable(tx.version)
	}

	return nil
}

func (tx *Tx) Rollback() error {
	if !tx.writable {
		return ErrNotWritable
	}

	defer tx.db.rwlock.Unlock()

	defer tx.version.Discard()

	return tx.db.wal.Rollback()
}
