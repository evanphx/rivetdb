package rivetdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestMemtable(t *testing.T) {
	n := neko.Start(t)

	key := []byte("this is a key")
	val := []byte("hello world value")

	n.It("stores values", func() {
		mt := NewMemTable()

		mt.Put(1, key, val)

		out, ok := mt.Get(1, key)
		require.True(t, ok)

		assert.Equal(t, val, out)
	})

	n.It("can delete values", func() {
		mt := NewMemTable()

		mt.Put(1, key, val)

		mt.Delete(2, key)

		_, ok := mt.Get(2, key)
		require.False(t, ok)
	})

	n.It("can preserves older deleted values", func() {
		mt := NewMemTable()

		mt.Put(1, key, val)

		mt.Delete(2, key)

		out, ok := mt.Get(1, key)
		require.True(t, ok)

		assert.Equal(t, val, out)
	})

	n.Meow()
}
