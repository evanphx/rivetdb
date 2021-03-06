package rivetdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/evanphx/rivetdb/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestDB(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	dbpath := filepath.Join(tmpdir, "db")

	buk := []byte("bucket")
	key := []byte("this is a key")
	key2 := []byte("another key")
	val := []byte("hello world value")

	n.It("handles an empty db", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		assert.Nil(t, b2)
	})

	n.It("can write values and see them", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		out := b.Get(key)

		assert.Equal(t, val, out)

		err = tx.Commit()
		require.NoError(t, err)
	})

	n.It("exposes values from previous transactions", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)

		out := b2.Get(key)
		assert.Equal(t, val, out)
	})

	n.It("doesn't expose pre-commit written values", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		_, err = tx.CreateBucket(buk)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.Begin(true)
		require.NoError(t, err)

		err = tx.Bucket(buk).Put(key, val)
		require.NoError(t, err)

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		out := tx2.Bucket(buk).Get(key)
		assert.Nil(t, out)
	})

	n.It("tracks the memory used by each pair", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)

		err = tx.Commit()
		require.NoError(t, err)

		assert.True(t, db.memoryBytes > 0)
	})

	n.It("flushes data to a new level 0 table", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.flushMemory()
		require.NoError(t, err)

		assert.Equal(t, 0, db.memoryBytes)

		path := filepath.Join(dbpath, "level0_0.sst")

		fi, err := os.Stat(path)
		require.NoError(t, err)

		assert.True(t, fi.Size() > 0)

		vi := db.state.Version.mem.AllEntries()

		assert.False(t, vi.Next())

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)
		assert.Equal(t, val, out)
	})

	n.It("flushes data to a new level 0 table automatically", func() {
		db, err := New(dbpath, Options{MemoryBuffer: 10})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		db.WaitIdle()

		assert.Equal(t, 1, db.Stats.NumFlushes)

		assert.Equal(t, 0, db.memoryBytes)

		path := filepath.Join(dbpath, "level0_0.sst")

		fi, err := os.Stat(path)
		require.NoError(t, err)

		assert.True(t, fi.Size() > 0)

		vi := db.state.Version.mem.AllEntries()

		assert.False(t, vi.Next())

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)
		assert.Equal(t, val, out)
	})

	n.It("writes values to a WAL log that can be recovered", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.unlock()
		require.NoError(t, err)

		r, err := NewWALReader(db.walFile)
		require.NoError(t, err)

		list, err := r.IntoList()
		require.NoError(t, err)

		v := list.AllEntries()
		require.True(t, v.Next())

		db2, err := New(dbpath, Options{})
		require.NoError(t, err)

		assert.Equal(t, int64(1), db2.state.Txid)

		tx2, err := db2.Begin(false)
		require.NoError(t, err)

		assert.Equal(t, int64(1), tx2.txid)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)

		assert.Equal(t, val, out)
	})

	n.It("doesn't recover rolled back WAL values", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		tx, err = db.Begin(true)
		require.NoError(t, err)

		b, err = tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key2, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.unlock()
		require.NoError(t, err)

		db2, err := New(dbpath, Options{})
		require.NoError(t, err)

		assert.Equal(t, int64(2), db2.state.Txid)

		tx2, err := db2.Begin(false)
		require.NoError(t, err)

		assert.Equal(t, tx.txid, tx2.txid)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)
		assert.Nil(t, out)
	})

	n.It("flushes values to L0 on close", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		fkey := b.vkey(key)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)

		_, err = os.Stat(db.walFile)
		require.NotNil(t, err)

		r, err := sstable.NewReader(filepath.Join(dbpath, "level0_0.sst"))
		require.NoError(t, err)

		out, err := r.Get(1, fkey)
		require.NoError(t, err)

		assert.Equal(t, val, out)
	})

	n.It("locks the path", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		_, err = New(dbpath, Options{})
		assert.Equal(t, ErrDBLocked, err)

		db.Close()
	})

	n.It("reloads the transaction id", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)

		db2, err := New(dbpath, Options{})
		require.NoError(t, err)

		assert.Equal(t, db.state.Txid, db2.state.Txid)
		assert.Equal(t, *db.readTxid, *db2.readTxid)
	})

	n.It("reloads access to existing tables", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)

		db2, err := New(dbpath, Options{})
		require.NoError(t, err)

		tx2, err := db2.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)

		assert.Equal(t, val, out)
	})

	n.It("rotates mem to imm and is still usable", func() {
		db, err := New(dbpath, Options{})
		require.NoError(t, err)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		tx.injectLocal()

		db.makeImmutable(tx.version)

		*db.readTxid = tx.txid

		ver := db.state.LoadVersion()

		assert.NotNil(t, ver.imm)

		tx2, err := db.Begin(false)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)

		assert.Equal(t, val, out)
	})

	n.Meow()
}
