package rivetdb

import (
	"sync"

	"github.com/evanphx/rivetdb/skiplist"
)

type MemTable struct {
	lock sync.RWMutex
	list *skiplist.SkipList
}

func NewMemTable() *MemTable {
	return &MemTable{
		list: skiplist.New(0),
	}
}

func (mt *MemTable) Put(ver int64, key, val []byte) {
	mt.lock.Lock()

	mt.list.Set(ver, key, val)

	mt.lock.Unlock()
}

func (mt *MemTable) Get(ver int64, key []byte) ([]byte, bool) {
	mt.lock.RLock()

	val, ok := mt.list.Get(ver, key)

	mt.lock.RUnlock()

	return val, ok
}

func (mt *MemTable) Delete(ver int64, key []byte) {
	mt.lock.Lock()

	mt.list.Delete(ver, key)

	mt.lock.Unlock()
}
