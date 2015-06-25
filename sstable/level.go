package sstable

import "math/rand"
import "os"

type Level struct {
	readers []*Reader
	size    int64
}

func NewLevel() *Level {
	return &Level{}
}

func (l *Level) Size() int64 {
	return l.size
}

func (l *Level) Add(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	l.size += stat.Size()

	r, err := NewReader(path)
	if err != nil {
		return err
	}

	l.readers = append(l.readers, r)

	return nil
}

func (l *Level) Remove(path string) {
	for idx, r := range l.readers {
		if r.Path == path {
			r.Close()

			l.readers = append(l.readers[:idx], l.readers[idx+1:]...)

			return
		}
	}
}

func (l *Level) Get(ver int64, key []byte) ([]byte, error) {
	for _, r := range l.readers {
		if r.MayContain(key) {
			return r.Get(ver, key)
		}
	}

	return nil, nil
}

func (l *Level) PickRandom() (string, KeyRange) {
	r := l.readers[rand.Intn(len(l.readers))]

	return r.Path, r.Range
}

func (l *Level) FindOverlap(rng KeyRange) []string {
	var paths []string

	for _, r := range l.readers {
		if r.Range.Overlap(rng) {
			paths = append(paths, r.Path)
		}
	}

	return paths
}
