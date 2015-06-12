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
	"math/rand"
)

// TODO(ryszard):
//   - A separately seeded source of randomness

// p is the fraction of nodes with level i pointers that also have
// level i+1 pointers. p equal to 1/4 is a good value from the point
// of view of speed and space requirements. If variability of running
// times is a concern, 1/2 is a better value for p.
const p = 0.25

const DefaultMaxLevel = 32

type version struct {
	ver int64
	val []byte
}

// A node is a container for key-value pairs that are stored in a skip
// list.
type node struct {
	forward  []*node
	backward *node
	key      []byte
	value    []version
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
	current := i.current
	list := i.list

	// If the existing iterator outside of the known key range, we should set the
	// position back to the beginning of the list.
	if current == nil {
		current = list.header
	}

	// If the target key occurs before the current key, we cannot take advantage
	// of the heretofore spent traversal cost to find it; resetting back to the
	// beginning is the safest choice.
	if current.key != nil && list.lessThan(key, current.key) {
		current = list.header
	}

	// We should back up to the so that we can seek to our present value if that
	// is requested for whatever reason.
	if current.backward == nil {
		current = list.header
	} else {
		current = current.backward
	}

	current = list.getPath(current, nil, key)

	if current == nil {
		return
	}

	for {
		val, ok := current.atLeast(i.ver)
		if ok {
			i.current = current
			i.key = current.key
			i.value = val

			return true
		}

		if !current.hasNext() {
			break
		}

		current = current.next()
	}

	return false
}

func (i *iter) Close() {
	i.key = nil
	i.value = nil
	i.current = nil
	i.list = nil
}

type rangeIterator struct {
	iter
	upperLimit []byte
	lowerLimit []byte
}

func (i *rangeIterator) Next() bool {
	for i.current.hasNext() {
		next := i.current.next()

		if !i.list.lessThan(next.key, i.upperLimit) {
			return false
		}

		i.current = i.current.next()
		i.key = i.current.key

		if val, ok := i.current.atLeast(i.ver); ok {
			i.value = val
			return true
		}
	}

	return false
}

func (i *rangeIterator) Previous() bool {
	for i.current.hasPrevious() {
		previous := i.current.previous()

		if i.list.lessThan(previous.key, i.lowerLimit) {
			return false
		}

		i.current = previous
		i.key = i.current.key

		if val, ok := i.current.atLeast(i.ver); ok {
			i.value = val
			return true
		}
	}

	return false
}

func (i *rangeIterator) Seek(key []byte) (ok bool) {
	if i.list.lessThan(key, i.lowerLimit) {
		return
	} else if !i.list.lessThan(key, i.upperLimit) {
		return
	}

	return i.iter.Seek(key)
}

func (i *rangeIterator) Close() {
	i.iter.Close()
	i.upperLimit = nil
	i.lowerLimit = nil
}

// Iterator returns an Iterator that will go through all elements s.
func (s *SkipList) Iterator() Iterator {
	return &iter{
		current: s.header,
		list:    s,
	}
}

func (i *iter) setValue(ver int64) bool {
	for {
		val, ok := i.current.atLeast(ver)
		if ok {
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
	current := s.getPath(s.header, nil, key)
	if current == nil {
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

// SeekToFirst returns a bidirectional iterator starting from the first element
// in the list if the list is populated; otherwise, a nil iterator is returned.
func (s *SkipList) SeekToFirst(ver int64) Iterator {
	if !s.header.hasNext() {
		return nil
	}

	current := s.header.next()

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
	current := s.footer
	if current == nil {
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

// Range returns an iterator that will go through all the
// elements of the skip list that are greater or equal than from, but
// less than to.
func (s *SkipList) Range(ver int64, from, to []byte) Iterator {
	start := s.getPath(s.header, nil, from)
	return &rangeIterator{
		iter: iter{
			ver: ver,
			current: &node{
				forward:  []*node{start},
				backward: start,
			},
			list: s,
		},
		upperLimit: to,
		lowerLimit: from,
	}
}

func (s *SkipList) level() int {
	return len(s.header.forward) - 1
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (s *SkipList) effectiveMaxLevel() int {
	return maxInt(s.level(), s.MaxLevel)
}

// Returns a new random level.
func (s SkipList) randomLevel() (n int) {
	for n = 0; n < s.effectiveMaxLevel() && rand.Float64() < p; n++ {
	}
	return
}

// Get returns the value associated with key from s (nil if the key is
// not present in s). The second return value is true when the key is
// present.
func (s *SkipList) Get(ver int64, key []byte) (value []byte, ok bool) {
	candidate := s.getPath(s.header, nil, key)

	if candidate == nil || !bytes.Equal(candidate.key, key) {
		return nil, false
	}

	val, ok := candidate.atLeast(ver)
	if ok {
		return val, true
	}

	return nil, false
}

// GetGreaterOrEqual finds the node whose key is greater than or equal
// to min. It returns its value, its actual key, and whether such a
// node is present in the skip list.
func (s *SkipList) GetGreaterOrEqual(ver int64, min []byte) (actualKey, value []byte, ok bool) {
	candidate := s.getPath(s.header, nil, min)

	if candidate != nil {
		if val, ok := candidate.atLeast(ver); ok {
			return candidate.key, val, true
		}
	}

	return nil, nil, false
}

// getPath populates update with nodes that constitute the path to the
// node that may contain key. The candidate node will be returned. If
// update is nil, it will be left alone (the candidate node will still
// be returned). If update is not nil, but it doesn't have enough
// slots for all the nodes in the path, getPath will panic.
func (s *SkipList) getPath(current *node, update []*node, key []byte) *node {
	depth := len(current.forward) - 1

	for i := depth; i >= 0; i-- {
		for current.forward[i] != nil && s.lessThan(current.forward[i].key, key) {
			current = current.forward[i]
		}
		if update != nil {
			update[i] = current
		}
	}
	return current.next()
}

// Sets set the value associated with key in s.
func (s *SkipList) Set(ver int64, key, value []byte) {
	if key == nil {
		panic("goskiplist: nil keys are not supported")
	}
	// s.level starts from 0, so we need to allocate one.
	update := make([]*node, s.level()+1, s.effectiveMaxLevel()+1)
	candidate := s.getPath(s.header, update, key)

	if candidate != nil && bytes.Equal(candidate.key, key) {
		candidate.addValue(ver, value)
		return
	}

	newLevel := s.randomLevel()

	if currentLevel := s.level(); newLevel > currentLevel {
		// there are no pointers for the higher levels in
		// update. Header should be there. Also add higher
		// level links to the header.
		for i := currentLevel + 1; i <= newLevel; i++ {
			update = append(update, s.header)
			s.header.forward = append(s.header.forward, nil)
		}
	}

	newNode := &node{
		forward: make([]*node, newLevel+1, s.effectiveMaxLevel()+1),
		key:     key,
		value:   []version{{ver, value}},
	}

	if previous := update[0]; previous.key != nil {
		newNode.backward = previous
	}

	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	if newNode.forward[0] != nil {
		if newNode.forward[0].backward != newNode {
			newNode.forward[0].backward = newNode
		}
	}

	if s.footer == nil || s.lessThan(s.footer.key, key) {
		s.footer = newNode
	}
}

// Delete removes the node with the given key.
//
// It returns the old value and whether the node was present.
func (s *SkipList) Delete(ver int64, key []byte) bool {
	if key == nil {
		panic("goskiplist: nil keys are not supported")
	}

	update := make([]*node, s.level()+1, s.effectiveMaxLevel())
	candidate := s.getPath(s.header, update, key)

	if candidate == nil || !bytes.Equal(candidate.key, key) {
		return false
	}

	candidate.addValue(ver, nil)

	return true
}

// NewCustomMap returns a new SkipList that will use lessThan as the
// comparison function. lessThan should define a linear order on keys
// you intend to use with the SkipList.
func New() *SkipList {
	return &SkipList{
		header: &node{
			forward: []*node{nil},
		},
		MaxLevel: DefaultMaxLevel,
	}
}
