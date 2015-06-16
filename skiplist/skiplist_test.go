// Copyright 2012 Google Inc. All rights reserved.
// Author: Ric Szopa (Ryszard) <ryszard.szopa@gmail.com>

// Package skiplist implements skip list based maps and sets.
//
// Skip lists are a data structure that can be used in place of
// balanced trees. Skip lists use probabilistic balancing rather than
// strictly enforced balancing and as a result the algorithms for
// insertion and deletion in skip lists are much simpler and
// significantly faster than equivalent algorithms for balanced trees.
//
// Skip lists were first described in Pugh, William (June 1990). "Skip
// lists: a probabilistic alternative to balanced
// trees". Communications of the ACM 33 (6): 668–676
package skiplist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektra/neko"
)

func TestSkiplist(t *testing.T) {
	n := neko.Start(t)

	n.It("seeks to the next key", func() {
		s := New(0)

		s.Set(1, ik(1), ik(1))
		s.Set(1, ik(3), ik(3))

		iter := s.Iterator(1)
		iter.Seek(ik(2))

		assert.Equal(t, ik(3), iter.Key())
		assert.Equal(t, ik(3), iter.Value())
	})

	n.Meow()
}

func (s *SkipList) printRepr() {

	fmt.Printf("header:\n")
	for i, link := range s.header.forward {
		if link != nil {
			fmt.Printf("\t%d: -> %v\n", i, link.key)
		} else {
			fmt.Printf("\t%d: -> END\n", i)
		}
	}

	for node := s.header.next(); node != nil; node = node.next() {
		fmt.Printf("%v: %v (level %d)\n", node.key, node.value, len(node.forward))
		for i, link := range node.forward {
			if link != nil {
				fmt.Printf("\t%d: -> %v\n", i, link.key)
			} else {
				fmt.Printf("\t%d: -> END\n", i)
			}
		}
	}
	fmt.Println()
}

func TestEmptyNodeNext(t *testing.T) {
	n := new(node)
	if next := n.next(); next != nil {
		t.Errorf("Next() should be nil for an empty node.")
	}

	if n.hasNext() {
		t.Errorf("hasNext() should be false for an empty node.")
	}
}

func TestEmptyNodePrev(t *testing.T) {
	n := new(node)
	if previous := n.previous(); previous != nil {
		t.Errorf("Previous() should be nil for an empty node.")
	}

	if n.hasPrevious() {
		t.Errorf("hasPrevious() should be false for an empty node.")
	}
}

func ik(n uint32) []byte {
	b := make([]byte, 4)

	binary.BigEndian.PutUint32(b, n)

	return b
}

func TestNodeHasNext(t *testing.T) {
	s := New(0)
	s.Set(1, ik(0), ik(0))
	node := s.header.next()
	if !bytes.Equal(node.key, ik(0)) {
		t.Fatalf("We got the wrong node: %v.", node)
	}

	if !node.hasNext() {
		t.Errorf("%v should be the last node.", node)
	}

	nxt := node.next()

	assert.Equal(t, nxt, s.footer)
}

func TestNodeHasPrev(t *testing.T) {
	s := New(0)
	s.Set(1, ik(0), ik(0))
	node := s.header.previous()
	if node != nil {
		t.Fatalf("Expected no previous entry, got %v.", node)
	}
}

func (s *SkipList) check(t *testing.T, key, wanted uint32) {
	if got, _ := s.Get(1, ik(key)); !bytes.Equal(got, ik(wanted)) {
		t.Errorf("For key %v wanted value %v, got %v.", key, wanted, got)
	}
}

func TestGet(t *testing.T) {
	s := New(0)
	s.Set(1, ik(0), ik(0))

	if value, present := s.Get(1, ik(0)); !(bytes.Equal(value, ik(0)) && present) {
		t.Errorf("%v, %v instead of %v, %v", value, present, 0, true)
	}

	if value, present := s.Get(1, ik(100)); value != nil || present {
		t.Errorf("%v, %v instead of %v, %v", value, present, nil, false)
	}
}

func TestSet(t *testing.T) {
	s := New(0)

	s.Set(1, ik(0), ik(0))
	s.Set(1, ik(1), ik(1))

	s.check(t, 0, 0)
	if t.Failed() {
		t.Errorf("header.Next() after s.Set(1, 0, 0) and s.Set(1, 1, 1): %v.", s.header.next())
	}
	s.check(t, 1, 1)

}

func TestChange(t *testing.T) {
	s := New(0)
	s.Set(1, ik(0), ik(0))
	s.Set(1, ik(1), ik(1))
	s.Set(1, ik(2), ik(2))

	s.Set(2, ik(0), ik(7))
	if value, _ := s.Get(2, ik(0)); !bytes.Equal(value, ik(7)) {
		t.Errorf("Value should be 7, not %d", value)
	}
	s.Set(2, ik(1), ik(8))
	if value, _ := s.Get(2, ik(1)); !bytes.Equal(value, ik(8)) {
		t.Errorf("Value should be 8, not %d", value)
	}

}

func TestDelete(t *testing.T) {
	s := New(0)
	for i := uint32(0); i < 10; i++ {
		s.Set(1, ik(i), ik(i))
	}
	for i := uint32(0); i < 10; i += 2 {
		s.Delete(2, ik(i))
	}

	for i := uint32(0); i < 10; i += 2 {
		if _, present := s.Get(2, ik(i)); present {
			t.Errorf("%d should not be present in s", i)
		}
	}
}

func TestIteration(t *testing.T) {
	s := New(0)
	for i := uint32(0); i < 20; i++ {
		s.Set(1, ik(i), ik(i))
	}

	seen := 0
	var lastKey []byte

	i := s.Iterator(1)
	defer i.Close()

	for i.Next() {
		seen++
		lastKey = i.Key()
		if !bytes.Equal(i.Key(), i.Value()) {
			t.Errorf("Wrong value for key %v: %v.", i.Key(), i.Value())
		}
	}

	for i.Previous() {
		if !bytes.Equal(i.Key(), i.Value()) {
			t.Errorf("Wrong value for key %v: %v.", i.Key(), i.Value())
		}

		if bytes.Compare(i.Key(), lastKey) > 0 {
			t.Errorf("Expected key to descend but ascended from %v to %v.", lastKey, i.Key())
		}

		lastKey = i.Key()
	}

	if !bytes.Equal(lastKey, ik(0)) {
		t.Errorf("Expected to count back to zero, but stopped at key %v.", lastKey)
	}
}

func TestSomeMore(t *testing.T) {
	s := New(0)
	insertions := [...]uint32{4, 1, 2, 9, 10, 7, 3}
	for _, i := range insertions {
		s.Set(1, ik(i), ik(i))
	}
	for _, i := range insertions {
		s.check(t, i, i)
	}

}

func makeRandomList(n int) *SkipList {
	s := New(0)
	for i := 0; i < n; i++ {
		insert := uint32(rand.Int())
		s.Set(1, ik(insert), ik(insert))
	}
	return s
}

func LookupBenchmark(b *testing.B, n int) {
	b.StopTimer()
	s := makeRandomList(n)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Get(1, ik(uint32(rand.Int())))
	}
}

func SetBenchmark(b *testing.B, n int) {
	b.StopTimer()
	values := []uint32{}
	for i := 0; i < b.N; i++ {
		values = append(values, uint32(rand.Int()))
	}
	s := New(0)
	b.StartTimer()
	for i := uint32(0); i < uint32(b.N); i++ {
		s.Set(1, ik(values[i]), ik(values[i]))
	}
}

// Make sure that all the keys are unique and are returned in order.
func TestSanity(t *testing.T) {
	s := New(0)
	for i := 0; i < 10000; i++ {
		insert := uint32(rand.Int())
		s.Set(1, ik(insert), ik(insert))
	}
	var last []byte

	i := s.Iterator(1)
	defer i.Close()

	for i.Next() {
		if last != nil && bytes.Compare(i.Key(), last) < 0 {
			t.Errorf("Not in order!")
		}
		last = i.Key()
	}

	for i.Previous() {
		if last != nil && bytes.Compare(i.Key(), last) > 0 {
			t.Errorf("Not in order!")
		}
		last = i.Key()
	}
}

func TestDeletingHighestLevelNodeDoesntBreakSkiplist(t *testing.T) {
	s := New(0)
	elements := []uint32{1, 3, 5, 7, 0, 4, 6, 10, 11}

	for _, i := range elements {
		s.Set(1, ik(i), ik(i))
	}

	highestLevelNode := s.footer.backward

	s.Delete(2, highestLevelNode.key)

	seen := 0
	i := s.Iterator(2)
	defer i.Close()

	for i.Next() {
		seen++
	}

	if seen == 0 {
		t.Errorf("Iteration is broken (no elements seen).")
	}
}

func TestIteratorPrevHoles(t *testing.T) {
	m := New(0)

	i := m.Iterator(1)
	defer i.Close()

	m.Set(1, ik(0), ik(0))
	m.Set(1, ik(1), ik(1))
	m.Set(1, ik(2), ik(2))

	if !i.Next() {
		t.Errorf("Expected iterator to move successfully to the next.")
	}

	if !i.Next() {
		t.Errorf("Expected iterator to move successfully to the next.")
	}

	if !i.Next() {
		t.Errorf("Expected iterator to move successfully to the next.")
	}

	if !bytes.Equal(i.Key(), ik(2)) {
		t.Errorf("Expected iterator to reach key 2 and value 2, got %v and %v.", i.Key(), i.Value())
	}

	if !i.Previous() {
		t.Errorf("Expected iterator to move successfully to the previous.")
	}

	if !bytes.Equal(i.Key(), ik(1)) {
		t.Errorf("Expected iterator to reach key 1 and value 1, got %v and %v.", i.Key(), i.Value())
	}

	if !i.Next() {
		t.Errorf("Expected iterator to move successfully to the next.")
	}
}
