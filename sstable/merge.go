package sstable

import (
	"bytes"
	"errors"
)

type indexIterator struct {
	r *Reader
	p int
}

func (i *indexIterator) key() []byte {
	return i.r.indexes[i.p].Key
}

func (i *indexIterator) ver() int64 {
	return i.r.indexes[i.p].GetVersion()
}

func (i *indexIterator) value() ([]byte, error) {
	idx := i.r.indexes[i.p]
	return i.r.Get(idx.GetVersion(), idx.GetKey())
}

func (i *indexIterator) next() bool {
	i.p++

	return i.p < len(i.r.indexes)
}

type Merger struct {
	iters []*indexIterator
}

func NewMerger() *Merger {
	return &Merger{}
}

func (m *Merger) Add(path string) error {
	r, err := NewReader(path)
	if err != nil {
		return err
	}

	m.iters = append(m.iters, &indexIterator{r: r})

	return nil
}

var ErrMultipleKeySameVersion = errors.New("multiple keys have the same version")

func (m *Merger) remove(next *indexIterator) {
	for idx, i := range m.iters {
		if i == next {
			m.iters = append(m.iters[:idx], m.iters[idx+1:]...)
			break
		}
	}
}

func (m *Merger) MergeInto(out string, min int64) error {
	w, err := NewWriter(out)
	if err != nil {
		return err
	}

	for len(m.iters) > 0 {
		var next *indexIterator

		var toRemove []*indexIterator

		var multiples bool

		for _, i := range m.iters {
			if next == nil {
				next = i
				continue
			}

			cmp := bytes.Compare(i.key(), next.key())

			switch cmp {
			case -1:
				next = i
			case 0:
				if next.ver() == i.ver() {
					return ErrMultipleKeySameVersion
				}

				if next.ver() < i.ver() {
					if next.ver() < min {
						if !next.next() {
							toRemove = append(toRemove, next)
						}
					} else {
						multiples = true
					}

					next = i
				} else {
					if i.ver() < min {
						if !i.next() {
							toRemove = append(toRemove, i)
						}
					} else {
						multiples = true
					}
				}
			}
		}

		for _, i := range toRemove {
			m.remove(i)
		}

		val, err := next.value()
		if err != nil {
			return err
		}

		if val != nil || multiples {
			err = w.Add(next.ver(), next.key(), val)
			if err != nil {
				return err
			}
		}

		if !next.next() {
			m.remove(next)
		}
	}

	return w.Close()
}
