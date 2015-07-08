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

// Concurrency added based on https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf
package skiplist

import (
	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
)

// TODO(ryszard):
//   - A separately seeded source of randomness

// p is the fraction of nodes with level i pointers that also have
// level i+1 pointers. p equal to 1/4 is a good value from the point
// of view of speed and space requirements. If variability of running
// times is a concern, 1/2 is a better value for p.
const p = 0.25

const DefaultMaxLevel = 4

type version struct {
	ver int64
	val []byte
}

// A node is a container for key-value pairs that are stored in a skip
// list.
type node struct {
	fullyLinked int32
	key         []byte
	value       []version
	topLayer    int
	forward     []*node
	backward    *node
	lock        sync.Mutex
}

// next returns the next node in the skip list containing n.
func (n *node) next() *node {
	if len(n.forward) == 0 {
		return nil
	}
	return n.forward[0]
}

// previous returns the previous node in the skip list containing n.
func (n *node) previous() *node {
	return n.backward
}

// hasNext returns true if n has a next node.
func (n *node) hasNext() bool {
	return n.next() != nil
}

// hasPrevious returns true if n has a previous node.
func (n *node) hasPrevious() bool {
	return n.previous() != nil
}

func (n *node) atLeast(ver int64) ([]byte, bool) {
	for i := len(n.value) - 1; i >= 0; i-- {
		if n.value[i].ver <= ver {
			v := n.value[i].val

			if v == nil {
				return nil, false
			}

			return v, true
		}
	}

	return nil, false
}

func (n *node) addValue(ver int64, val []byte) {
	if n.value[len(n.value)-1].ver >= ver {
		panic("adding older (or same) version")
	}

	n.value = append(n.value, version{ver, val})
}

func (n *node) linked() bool {
	return atomic.LoadInt32(&n.fullyLinked) == 1
}

func (n *node) setLinked() {
	atomic.StoreInt32(&n.fullyLinked, 1)
}

// A SkipList is a map-like data structure that maintains an ordered
// collection of key-value pairs. Insertion, lookup, and deletion are
// all O(log n) operations. A SkipList can efficiently store up to
// 2^MaxLevel items.
//
// To iterate over a skip list (where s is a
// *SkipList):
//
//	for i := s.Iterator(); i.Next(); {
//		// do something with i.Key() and i.Value()
//	}
type SkipList struct {
	header *node
	footer *node

	// MaxLevel determines how many items the SkipList can store
	// efficiently (2^MaxLevel).
	//
	// It is safe to increase MaxLevel to accomodate more
	// elements. If you decrease MaxLevel and the skip list
	// already contains nodes on higer levels, the effective
	// MaxLevel will be the greater of the new MaxLevel and the
	// level of the highest node.
	//
	// A SkipList with MaxLevel equal to 0 is equivalent to a
	// standard linked list and will not have any of the nice
	// properties of skip lists (probably not what you want).
	MaxLevel int

	searches sync.Pool
}

// NewCustomMap returns a new SkipList that will use lessThan as the
// comparison function. lessThan should define a linear order on keys
// you intend to use with the SkipList.
func New(max int) *SkipList {
	if max == 0 {
		max = DefaultMaxLevel
	}

	sk := &SkipList{
		header: &node{
			forward: make([]*node, max+1),
		},
		footer:   &node{},
		MaxLevel: max,
	}

	sk.searches.New = func() interface{} {
		return make([]*node, max)
	}

	for i := 0; i < max; i++ {
		sk.header.forward[i] = sk.footer
	}

	return sk
}

func (s *SkipList) lessThan(key, val []byte) bool {
	return bytes.Compare(key, val) < 0
}

// Iterator is an interface that you can use to iterate through the
// skip list (in its entirety or fragments). For an use example, see
// the documentation of SkipList.
//
// Key and Value return the key and the value of the current node.
type Iterator interface {
	// Next returns true if the iterator contains subsequent elements
	// and advances its state to the next element if that is possible.
	Next() (ok bool)
	// Previous returns true if the iterator contains previous elements
	// and rewinds its state to the previous element if that is possible.
	Previous() (ok bool)
	// Key returns the current key.
	Key() []byte
	// Value returns the current value.
	Value() []byte
	// Seek reduces iterative seek costs for searching forward into the Skip List
	// by remarking the range of keys over which it has scanned before.  If the
	// requested key occurs prior to the point, the Skip List will start searching
	// as a safeguard.  It returns true if the key is within the known range of
	// the list.
	Seek(key []byte) (ok bool)
	// Close this iterator to reap resources associated with it.  While not
	// strictly required, it will provide extra hints for the garbage collector.
	Close()
}

type iter struct {
	ver     int64
	current *node
	key     []byte
	list    *SkipList
	value   []byte
}

func (i iter) Key() []byte {
	return i.key
}

func (i iter) Value() []byte {
	return i.value
}

func (i *iter) Next() bool {
	for i.current.hasNext() {
		i.current = i.current.next()
		i.key = i.current.key

		val, ok := i.current.atLeast(i.ver)
		if ok {
			i.value = val
			return true
		}
	}

	return false
}

func (i *iter) Previous() bool {
	for i.current.hasPrevious() {
		i.current = i.current.previous()
		i.key = i.current.key

		val, ok := i.current.atLeast(i.ver)
		if ok {
			i.value = val
			return true
		}
	}

	return false
}

func (i *iter) Seek(key []byte) (ok bool) {
	preds := i.list.searches.Get().([]*node)
	succs := i.list.searches.Get().([]*node)

	defer i.list.searches.Put(preds)
	defer i.list.searches.Put(succs)

	layer, found := i.list.findNode(key, preds, succs)
	if !found {
		layer = 0
	}

	node := succs[layer]

	for !node.linked() {
		if !node.hasNext() {
			return false
		}

		node = node.next()
	}

	i.current = node

	return i.setValue(i.ver)
}

func (i *iter) Close() {
	i.key = nil
	i.value = nil
	i.current = nil
	i.list = nil
}

func (i *iter) Valid() bool {
	return i.current != nil
}

// Iterator returns an Iterator that will go through all elements s.
func (s *SkipList) Iterator(ver int64) Iterator {
	return &iter{
		ver:     ver,
		current: s.header,
		list:    s,
	}
}

func (s *SkipList) AllEntries() *VersionIterator {
	return &VersionIterator{current: s.header, list: s}
}

type VersionIterator struct {
	current *node
	list    *SkipList
}

func (i *VersionIterator) Next() bool {
	nxt := i.current.next()
	if nxt == nil {
		return false
	}

	if nxt == i.list.footer {
		return false
	}

	i.current = nxt
	return true
}

func (i *VersionIterator) Key() []byte {
	return i.current.key
}

func (i *VersionIterator) NumValues() int {
	return len(i.current.value)
}

func (i *VersionIterator) Value(v int) ([]byte, int64) {
	ver := i.current.value[v]
	return ver.val, ver.ver
}

func (i *iter) setValue(ver int64) bool {
	for {
		val, ok := i.current.atLeast(ver)
		if ok {
			i.key = i.current.key
			i.value = val
			return true
		}

		if !i.current.hasNext() {
			break
		}

		i.current = i.current.next()
	}

	return false
}

func (i *iter) setValueBackward(ver int64) bool {
	for {
		val, ok := i.current.atLeast(ver)
		if ok {
			i.value = val
			return true
		}

		if !i.current.hasPrevious() {
			break
		}

		i.current = i.current.previous()
	}

	return false
}

// Seek returns a bidirectional iterator starting with the first element whose
// key is greater or equal to key; otherwise, a nil iterator is returned.
func (s *SkipList) Seek(ver int64, key []byte) Iterator {
	iter := s.Iterator(ver)

	if iter.Seek(key) {
		return nil
	}

	return iter
}

// SeekToFirst returns a bidirectional iterator starting from the first element
// in the list if the list is populated; otherwise, a nil iterator is returned.
func (s *SkipList) SeekToFirst(ver int64) Iterator {
	if !s.header.hasNext() {
		return nil
	}

	current := s.header.next()

	if current == s.footer {
		return nil
	}

	iter := &iter{
		ver:     ver,
		current: current,
		key:     current.key,
		list:    s,
	}

	if iter.setValue(ver) {
		return iter
	}

	return nil
}

// SeekToLast returns a bidirectional iterator starting from the last element
// in the list if the list is populated; otherwise, a nil iterator is returned.
func (s *SkipList) SeekToLast(ver int64) Iterator {
	current := s.footer.backward
	if current == s.header {
		return nil
	}

	iter := &iter{
		ver:     ver,
		current: current,
		key:     current.key,
		list:    s,
	}

	if iter.setValueBackward(ver) {
		return iter
	}

	return nil
}

// Returns a new random level.
func (s *SkipList) randomLevel() int {
	lvl := 0

	for rand.Float64() < p && lvl < s.MaxLevel-1 {
		lvl++
	}

	return lvl
}

// Get returns the value associated with key from s (nil if the key is
// not present in s). The second return value is true when the key is
// present.
func (s *SkipList) Get(ver int64, key []byte) (value []byte, ok bool) {
	preds := s.searches.Get().([]*node)
	succs := s.searches.Get().([]*node)

	defer s.searches.Put(preds)
	defer s.searches.Put(succs)

	layer, found := s.findNode(key, preds, succs)
	if !found {
		return nil, false
	}

	node := succs[layer]

	if !node.linked() {
		return nil, false
	}

	return node.atLeast(ver)
}

func (s *SkipList) Contains(ver int64, key []byte) bool {
	_, ok := s.Get(ver, key)
	return ok
}

func (s *SkipList) findNode(v []byte, preds, succs []*node) (atLayer int, found bool) {
	pred := s.header

	for layer := s.MaxLevel - 1; layer >= 0; layer-- {
		curr := pred.forward[layer]

		for curr.key != nil && s.lessThan(curr.key, v) {
			pred = curr
			curr = pred.forward[layer]
		}

		if !found && curr.key != nil && bytes.Equal(v, curr.key) {
			atLayer = layer
			found = true
		}

		preds[layer] = pred
		succs[layer] = curr
	}

	return
}

func (s *SkipList) Set(ver int64, key, value []byte) bool {
	topLayer := s.randomLevel()

	preds := s.searches.Get().([]*node)
	succs := s.searches.Get().([]*node)

	defer s.searches.Put(preds)
	defer s.searches.Put(succs)

	for {
		layer, found := s.findNode(key, preds, succs)
		if found {
			nodeFound := succs[layer]

			for !nodeFound.linked() {
				runtime.Gosched()
			}

			nodeFound.addValue(ver, value)
			return true
		}

		highestLocked := -1

		var prevPred *node

		valid := true

		for layer := 0; valid && layer <= topLayer; layer++ {
			pred := preds[layer]
			succ := succs[layer]

			if pred != prevPred {
				pred.lock.Lock()
				highestLocked = layer
				prevPred = pred
			}

			valid = pred.forward[layer] == succ
		}

		if !valid {
			for level := 0; level <= highestLocked; level++ {
				pred := preds[level]
				pred.lock.Unlock()
			}

			continue
		}

		newNode := &node{
			topLayer: topLayer,
			forward:  make([]*node, topLayer+1),
			key:      key,
			value:    []version{{ver, value}},
		}

		if previous := preds[0]; previous.key != nil {
			newNode.backward = previous
		}

		for layer := 0; layer <= topLayer; layer++ {
			newNode.forward[layer] = succs[layer]
		}

		for layer := 0; layer <= topLayer; layer++ {
			preds[layer].forward[layer] = newNode
		}

		if newNode.forward[0] != nil {
			if newNode.forward[0].backward != newNode {
				newNode.forward[0].backward = newNode
			}
		}

		newNode.setLinked()

		prevPred = nil

		for level := 0; level <= highestLocked; level++ {
			pred := preds[level]

			if pred != prevPred {
				pred.lock.Unlock()
				prevPred = pred
			}
		}

		return true
	}
}

// Delete removes the node with the given key.
func (s *SkipList) Delete(ver int64, key []byte) {
	s.Set(ver, key, nil)
}

func (s *SkipList) Move(other *SkipList) error {
	all := other.AllEntries()

	for all.Next() {
		for i := 0; i < all.NumValues(); i++ {
			val, ver := all.Value(i)

			s.Set(ver, all.Key(), val)
		}
	}

	return nil
}
