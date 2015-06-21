package rivetdb

import (
	"github.com/evanphx/rivetdb/skiplist"
	"github.com/evanphx/rivetdb/sstable"
)

type ZeroWriter struct {
	w    *sstable.Writer
	skip *skiplist.SkipList
}

func NewZeroWriter(path string, skip *skiplist.SkipList) (*ZeroWriter, error) {
	s, err := sstable.NewWriter(path)
	if err != nil {
		return nil, err
	}

	w := &ZeroWriter{w: s, skip: skip}

	return w, nil
}

func (w *ZeroWriter) Write() (*KeyRange, error) {
	i := w.skip.AllEntries()

	var (
		first []byte
		last  []byte
	)

	for i.Next() {
		if first == nil {
			first = i.Key()
		} else {
			last = i.Key()
		}

		for j := 0; j < i.NumValues(); j++ {
			val, ver := i.Value(j)

			err := w.w.Add(ver, i.Key(), val)
			if err != nil {
				return nil, err
			}
		}
	}

	err := w.w.Close()
	if err != nil {
		return nil, err
	}

	// Edge case for there being only one key
	if last == nil {
		last = first
	}

	return &KeyRange{first, last}, nil
}
