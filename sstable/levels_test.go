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

	n.It("pushes levels that are too large into the next level", func() {
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

		levels := NewLevels(3)

		levels.Add(1, path)
		levels.Add(2, path1a)

		pathout := filepath.Join(tmpdir, "out")
		defer os.Remove(pathout)

		levels.levels[1].size = 10 * 1024 * 1024

		err = levels.ConsiderMerges(tmpdir, 4)
		require.NoError(t, err)

		assert.Equal(t, 0, len(levels.At(1).readers))
		assert.Equal(t, 1, len(levels.At(2).readers))

		v, err := levels.GetValue(2, []byte("2"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value2"), v)
	})

	n.It("merges all overlaping files in level 0", func() {
		path0a := filepath.Join(tmpdir, "0a")
		defer os.Remove(path0a)

		w, err := NewWriter(path0a)
		require.NoError(t, err)

		err = w.Add(2, []byte("1"), []byte("value1"))
		require.NoError(t, err)

		err = w.Add(2, []byte("5"), []byte("value5"))
		require.NoError(t, err)

		w.Close()

		path0b := filepath.Join(tmpdir, "0b")
		defer os.Remove(path0b)

		w, err = NewWriter(path0b)
		require.NoError(t, err)

		err = w.Add(2, []byte("3"), []byte("value3"))
		require.NoError(t, err)

		err = w.Add(2, []byte("4"), []byte("value4"))
		require.NoError(t, err)

		w.Close()

		path1a := filepath.Join(tmpdir, "1a")
		defer os.Remove(path1a)

		w, err = NewWriter(path1a)
		require.NoError(t, err)

		err = w.Add(1, []byte("1"), []byte("old1"))
		require.NoError(t, err)

		err = w.Add(1, []byte("6"), []byte("value6"))
		require.NoError(t, err)

		w.Close()

		levels := NewLevels(3)

		levels.Add(0, path0a)
		levels.Add(0, path0b)
		levels.Add(1, path1a)

		levels.levels[0].size = 4 * 1024 * 1024

		err = levels.ConsiderMerges(tmpdir, 4)
		require.NoError(t, err)

		assert.Equal(t, 0, len(levels.At(0).readers))
		assert.Equal(t, 1, len(levels.At(1).readers))

		v, err := levels.GetValue(2, []byte("5"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value5"), v)

		v, err = levels.GetValue(2, []byte("4"))
		require.NoError(t, err)

		assert.Equal(t, []byte("value4"), v)
	})

	n.Meow()
}
