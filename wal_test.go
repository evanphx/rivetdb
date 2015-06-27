package rivetdb

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestWAL(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	path := filepath.Join(tmpdir, "wal")

	n.Cleanup(func() {
		os.Remove(path)
	})

	n.It("records entries added", func() {
		wal, err := NewWAL(path)
		require.NoError(t, err)

		err = wal.Add(1, []byte{2}, []byte{3})
		require.NoError(t, err)

		data, err := ioutil.ReadFile(path)
		require.NoError(t, err)

		entSz := int(binary.BigEndian.Uint32(data[len(data)-4:]))

		var ent LogEntry

		data = data[:len(data)-4]

		err = ent.Unmarshal(data[len(data)-entSz:])
		require.NoError(t, err)

		assert.Equal(t, WALAppend, ent.GetOp())
		assert.Equal(t, int64(1), ent.GetVersion())
		assert.Equal(t, []byte{2}, ent.Key)
		assert.Equal(t, []byte{3}, ent.Value)
	})

	n.It("records a commit entry", func() {
		wal, err := NewWAL(path)
		require.NoError(t, err)

		err = wal.Commit(1)
		require.NoError(t, err)

		data, err := ioutil.ReadFile(path)
		require.NoError(t, err)

		entSz := int(binary.BigEndian.Uint32(data[len(data)-4:]))

		var ent LogEntry

		data = data[:len(data)-4]

		err = ent.Unmarshal(data[len(data)-entSz:])
		require.NoError(t, err)

		assert.Equal(t, WALCommit, ent.GetOp())
		assert.Equal(t, int64(1), ent.GetVersion())
	})

	n.It("can reconstitute the WAL into a skiplist", func() {
		wal, err := NewWAL(path)
		require.NoError(t, err)

		err = wal.Add(1, []byte{2}, []byte{3})
		require.NoError(t, err)

		err = wal.Commit(1)
		require.NoError(t, err)

		r, err := NewWALReader(path)
		require.NoError(t, err)

		list, err := r.IntoList()
		require.NoError(t, err)

		v, ok := list.Get(1, []byte{2})
		require.True(t, ok)

		assert.Equal(t, []byte{3}, v)
	})

	n.It("skips uncommited entries when reconstituting", func() {
		wal, err := NewWAL(path)
		require.NoError(t, err)

		err = wal.Add(1, []byte{2}, []byte{3})
		require.NoError(t, err)

		err = wal.Commit(1)
		require.NoError(t, err)

		err = wal.Add(2, []byte{4}, []byte{5})
		require.NoError(t, err)

		err = wal.Sync()
		require.NoError(t, err)

		r, err := NewWALReader(path)
		require.NoError(t, err)

		list, err := r.IntoList()
		require.NoError(t, err)

		_, ok := list.Get(1, []byte{4})
		require.False(t, ok)
	})

	n.Meow()
}
