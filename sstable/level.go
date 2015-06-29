package sstable

import (
	"math/rand"
	"os"
)

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

type LevelEdit struct {
	Add    []string
	Remove []string
}

func (l *Level) Edit(e LevelEdit) (*Level, error) {
	level := NewLevel()

	for _, r := range l.readers {
		add := true

		for _, p := range e.Remove {
			if r.Path == p {
				add = false
				break
			}
		}

		if add {
			r.Ref()
			level.readers = append(level.readers, r)
		}
	}

	for _, path := range e.Add {
		err := level.Add(path)
		if err != nil {
			return nil, err
		}
	}

	return level, nil
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
