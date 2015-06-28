package sstable

import (
	"errors"
	"fmt"
	"path/filepath"
)

type Levels struct {
	levels []*Level
}

func NewLevels(max int) *Levels {
	l := &Levels{
		levels: make([]*Level, max),
	}

	for idx, _ := range l.levels {
		l.levels[idx] = NewLevel()
	}

	return l
}

func (l *Levels) Add(num int, path string) error {
	return l.levels[num].Add(path)
}

func (l *Levels) At(num int) *Level {
	return l.levels[num]
}

func (l *Levels) GetValue(ver int64, key []byte) ([]byte, error) {
	for _, level := range l.levels {
		v, _ := level.Get(ver, key)
		if v != nil {
			return v, nil
		}
	}

	return nil, nil
}

type MergeRequest struct {
	Level      int
	File       string
	MinVersion int64
}

var ErrTopLevel = errors.New("top level, nowhere to merge to")

func (l *Levels) Merge(req MergeRequest) error {
	if req.Level >= len(l.levels)-1 {
		return ErrTopLevel
	}

	path, rng := l.levels[req.Level].PickRandom()

	merge := NewMerger()

	var l0paths []string

	if req.Level == 0 {
		overlap := l.levels[req.Level].FindOverlap(rng)

		for _, path := range overlap {
			err := merge.Add(path)
			if err != nil {
				return err
			}
		}

		l0paths = overlap
	} else {
		err := merge.Add(path)
		if err != nil {
			return err
		}

		l0paths = []string{path}
	}

	upLevel := req.Level + 1

	up := l.levels[upLevel]

	overlap := up.FindOverlap(rng)

	for _, path := range overlap {
		err := merge.Add(path)
		if err != nil {
			return err
		}
	}

	err := merge.MergeInto(req.File, req.MinVersion)
	if err != nil {
		return err
	}

	for _, path := range l0paths {
		l.levels[req.Level].Remove(path)
	}

	for _, path := range overlap {
		l.levels[upLevel].Remove(path)
	}

	return l.levels[upLevel].Add(req.File)
}

const (
	cMeg        int64 = 1024 * 1024
	cLevel0Size int64 = 4 * cMeg
)

func levelMax(level int) int64 {
	if level == 0 {
		return cLevel0Size
	}

	megs := int64(10)

	for i := 1; i < level; i++ {
		megs *= 10
	}

	return megs * cMeg
}

func (ls *Levels) ConsiderMerges(dir string, ver int64) error {
	for idx, l := range ls.levels {

		if l.Size() >= levelMax(idx) {
			file := filepath.Join(dir, fmt.Sprintf("level%d_%d.sst", idx, ver))

			err := ls.Merge(
				MergeRequest{
					Level:      idx,
					MinVersion: ver,
					File:       file,
				})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
