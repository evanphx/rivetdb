package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestSSTable(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	n.It("can write out a new table based on requests", func() {
		path := filepath.Join(tmpdir, "new")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add([]byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add([]byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		fi, err := os.Stat(path)
		require.NoError(t, err)

		assert.True(t, fi.Size() > 0)

		f, err := os.Open(path)
		require.NoError(t, err)

		buf := make([]byte, 1024)

		var hdr FileHeader

		n, err := f.Read(buf)
		require.NoError(t, err)

		assert.Equal(t, 1024, n)

		hsz := binary.BigEndian.Uint16(buf)

		err = hdr.Unmarshal(buf[2 : 2+hsz])

		assert.Equal(t, uint64(2), hdr.GetKeys())

		idx := hdr.GetIndex()

		_, err = f.Seek(int64(idx), os.SEEK_SET)
		require.NoError(t, err)

		data := make([]byte, hdr.GetIndexSize())

		n, err = io.ReadFull(f, data)
		require.NoError(t, err)

		assert.Equal(t, int(hdr.GetIndexSize()), n)

		e1Sz, sz := binary.Uvarint(data)

		var ie IndexEntry

		err = ie.Unmarshal(data[sz : sz+int(e1Sz)])
		require.NoError(t, err)

		assert.Equal(t, []byte("1"), ie.GetKey())

		e1 := ie.GetOffset()

		var entry Entry

		_, err = f.Seek(int64(e1), os.SEEK_SET)
		require.NoError(t, err)

		br := bufio.NewReader(f)

		entSz, err := binary.ReadUvarint(br)
		require.NoError(t, err)

		ebuf := make([]byte, entSz)

		_, err = br.Read(ebuf)
		require.NoError(t, err)

		err = entry.Unmarshal(ebuf)
		require.NoError(t, err)

		assert.Equal(t, []byte("1"), entry.GetKey())
		assert.Equal(t, []byte("value1"), entry.GetValue())
	})

	n.It("can read data from a table", func() {
		path := filepath.Join(tmpdir, "new")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add([]byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add([]byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		r, err := NewReader(path)
		require.NoError(t, err)

		v, err := r.Get([]byte("1"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value1"), v)
	})

	n.Meow()
}
