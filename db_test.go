package rivetdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestDB(t *testing.T) {
	n := neko.Start(t)

	buk := []byte("bucket")
	key := []byte("this is a key")
	val := []byte("hello world value")

	n.It("can write values and see them", func() {
		db := New()

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
		db := New()

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
		db := New()

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

	n.Meow()
}
