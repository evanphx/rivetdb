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
	val := []byte("hello world value")

	n.It("handles an empty db", func() {
		db := New(dbpath, Options{})

		defer os.RemoveAll(dbpath)

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		assert.Nil(t, b2)
	})

	n.It("can write values and see them", func() {
		db := New(dbpath, Options{})

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
		db := New(dbpath, Options{})

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
		db := New(dbpath, Options{})

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
		db := New(dbpath, Options{})

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
		db := New(dbpath, Options{})

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

		vi := db.skip.AllEntries()

		assert.False(t, vi.Next())

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)
		assert.Equal(t, val, out)
	})

	n.It("flushes data to a new level 0 table automatically", func() {
		opts := Options{
			MemoryBuffer: 10,
		}

		db := New(dbpath, opts)

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		assert.Equal(t, 1, db.Stats.NumFlushes)

		assert.Equal(t, 0, db.memoryBytes)

		path := filepath.Join(dbpath, "level0_0.sst")

		fi, err := os.Stat(path)
		require.NoError(t, err)

		assert.True(t, fi.Size() > 0)

		vi := db.skip.AllEntries()

		assert.False(t, vi.Next())

		tx2, err := db.Begin(false)
		require.NoError(t, err)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)
		assert.Equal(t, val, out)
	})

	n.It("writes values to a WAL log that can be recovered", func() {
		db := New(dbpath, Options{})

		defer os.RemoveAll(dbpath)

		tx, err := db.Begin(true)
		require.NoError(t, err)

		b, err := tx.CreateBucket(buk)
		require.NoError(t, err)

		err = b.Put(key, val)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		r, err := NewWALReader(db.walFile)
		require.NoError(t, err)

		list, err := r.IntoList()
		require.NoError(t, err)

		v := list.AllEntries()
		require.True(t, v.Next())

		db2 := New(dbpath, Options{})

		assert.Equal(t, int64(1), db2.txid)

		tx2, err := db2.Begin(false)
		require.NoError(t, err)

		assert.Equal(t, int64(1), tx2.txid)

		b2 := tx2.Bucket(buk)
		require.NotNil(t, b2)

		out := b2.Get(key)

		assert.Equal(t, val, out)
	})

	n.It("flushes values to L0 on close", func() {
		db := New(dbpath, Options{Debug: true})

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

	n.Meow()
}
