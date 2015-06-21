package sstable

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestMerge(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	n.It("creates a new table as a merger of other tables", func() {
		path := filepath.Join(tmpdir, "one")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add(1, []byte("2"), []byte("value2"))
		require.NoError(t, err)

		err = w.Add(3, []byte("4"), []byte("new 4"))
		require.NoError(t, err)

		w.Close()

		path2 := filepath.Join(tmpdir, "two")
		defer os.Remove(path2)

		q, err := NewWriter(path2)
		require.NoError(t, err)

		err = q.Add(1, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = q.Add(2, []byte("3"), []byte("value3"))
		require.NoError(t, err)

		err = q.Add(2, []byte("4"), []byte("old 4"))
		require.NoError(t, err)

		q.Close()

		merge := NewMerger()

		merge.Add(path)
		merge.Add(path2)

		path3 := filepath.Join(tmpdir, "three")
		defer os.Remove(path3)

		err = merge.MergeInto(path3)
		require.NoError(t, err)

		r, err := NewReader(path3)
		require.NoError(t, err)

		v, err := r.Get(1, []byte("1"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value1"), v)

		v, err = r.Get(1, []byte("2"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value2"), v)

		v, err = r.Get(3, []byte("3"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value3"), v)

		v, err = r.Get(3, []byte("4"))
		require.NoError(t, err)

		assert.Equal(t, []byte("new 4"), v)
	})

	n.Meow()
}
