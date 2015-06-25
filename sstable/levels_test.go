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

func TestLevel(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	n.It("holds readers for the tables in that level", func() {
		l := NewLevel()

		path := filepath.Join(tmpdir, "new")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add(1, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(2, []byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		err = l.Add(path)
		require.NoError(t, err)

		fi, err := os.Stat(path)
		require.NoError(t, err)

		assert.Equal(t, fi.Size(), l.Size())

		v, err := l.Get(1, []byte("1"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value1"), v)
	})

	n.It("can pick a random table", func() {
		l := NewLevel()

		path := filepath.Join(tmpdir, "new")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add(1, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(2, []byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		err = l.Add(path)
		require.NoError(t, err)

		path2 := filepath.Join(tmpdir, "new2")
		defer os.Remove(path)

		w, err = NewWriter(path2)
		require.NoError(t, err)

		err = w.Add(1, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(2, []byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		err = l.Add(path2)
		require.NoError(t, err)

		var (
			one int
			two int
		)

		for i := 0; i < 100; i++ {
			pick, _ := l.PickRandom()
			if pick == path {
				one++
			} else {
				two++
			}
		}

		assert.NotEqual(t, 0, one)
		assert.NotEqual(t, 0, two)
	})

	n.Meow()
}

func TestLevels(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "sstable")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	n.It("merges keys into higher levels", func() {
		path := filepath.Join(tmpdir, "0a")
		defer os.Remove(path)

		w, err := NewWriter(path)
		require.NoError(t, err)

		err = w.Add(1, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(2, []byte("2"), []byte("value2"))
		require.NoError(t, err)

		w.Close()

		path1a := filepath.Join(tmpdir, "1a")
		defer os.Remove(path1a)

		w, err = NewWriter(path1a)
		require.NoError(t, err)

		err = w.Add(3, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(4, []byte("3"), []byte("value3"))
		require.NoError(t, err)

		w.Close()

		levels := NewLevels(2)

		levels.Add(0, path)
		levels.Add(1, path1a)

		pathout := filepath.Join(tmpdir, "out")
		defer os.Remove(pathout)

		err = levels.Merge(MergeRequest{Level: 0, File: pathout})
		require.NoError(t, err)

		assert.Equal(t, 0, len(levels.At(0).readers))
		assert.Equal(t, 1, len(levels.At(1).readers))

		v, err := levels.GetValue(2, []byte("2"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value2"), v)
	})

	n.Meow()
}
