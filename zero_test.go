package rivetdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/evanphx/rivetdb/skiplist"
	"github.com/evanphx/rivetdb/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestZeroWriter(t *testing.T) {
	n := neko.Start(t)

	tmpdir, err := ioutil.TempDir("", "zero")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	n.It("writes a skiplist to disk", func() {
		k := []byte("k1")
		v := []byte("value1")

		path := filepath.Join(tmpdir, "new")

		skip := skiplist.New(0)
		skip.Set(1, k, v)
		skip.Set(1, []byte("k2"), v)
		skip.Set(1, []byte("k3"), v)

		w, err := NewZeroWriter(path, skip)
		require.NoError(t, err)

		rng, err := w.Write()
		require.NoError(t, err)

		assert.Equal(t, k, rng.Start)
		assert.Equal(t, []byte("k3"), rng.End)

		r, err := sstable.NewReader(path)
		require.NoError(t, err)

		out, err := r.Get(1, k)
		require.NoError(t, err)

		assert.Equal(t, v, out)
	})

	n.Meow()

}
