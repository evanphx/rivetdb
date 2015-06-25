package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektra/neko"
)

func TestKeyRange(t *testing.T) {
	n := neko.Start(t)

	n.It("returns true if range a is enclosed within range b", func() {
		a := KeyRange{Start: []byte{2}, End: []byte{3}}
		b := KeyRange{Start: []byte{1}, End: []byte{4}}

		assert.True(t, a.Overlap(b))
		assert.True(t, b.Overlap(a))
	})

	n.It("returns true if range a is an subset of range b at the end", func() {
		a := KeyRange{Start: []byte{2}, End: []byte{4}}
		b := KeyRange{Start: []byte{1}, End: []byte{4}}

		assert.True(t, a.Overlap(b))
		assert.True(t, b.Overlap(a))
	})

	n.It("returns true if range a is an subset of range b at the beginning", func() {
		a := KeyRange{Start: []byte{1}, End: []byte{2}}
		b := KeyRange{Start: []byte{1}, End: []byte{4}}

		assert.True(t, a.Overlap(b))
		assert.True(t, b.Overlap(a))
	})

	n.It("returns true if range a has a subrange within b", func() {
		a := KeyRange{Start: []byte{1}, End: []byte{3}}
		b := KeyRange{Start: []byte{2}, End: []byte{5}}

		assert.True(t, a.Overlap(b))
		assert.True(t, b.Overlap(a))
	})

	n.It("returns false if range a is entirely before range b", func() {
		a := KeyRange{Start: []byte{1}, End: []byte{3}}
		b := KeyRange{Start: []byte{4}, End: []byte{5}}

		assert.False(t, a.Overlap(b))
		assert.False(t, b.Overlap(a))
	})

	n.It("returns false if range a is entirely after range b", func() {
		a := KeyRange{Start: []byte{4}, End: []byte{6}}
		b := KeyRange{Start: []byte{1}, End: []byte{3}}

		assert.False(t, a.Overlap(b))
		assert.False(t, b.Overlap(a))
	})

	n.Meow()
}
