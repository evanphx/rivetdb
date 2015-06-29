package sstable

import (
	"encoding/json"
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

type levelState struct {
	Paths []string `json:"path"`
}

type state struct {
	Levels []levelState `json:"levels"`
}

func (l *Levels) MarshalJSON() ([]byte, error) {
	var s state

	for _, level := range l.levels {
		ls := levelState{}

		for _, r := range level.readers {
			ls.Paths = append(ls.Paths, r.Path)
		}

		s.Levels = append(s.Levels, ls)
	}

	return json.Marshal(&s)
}

func (l *Levels) UnmarshalJSON(data []byte) error {
	var s state

	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	for idx, level := range l.levels {
		ls := s.Levels[idx]

		for _, path := range ls.Paths {
			err = level.Add(path)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *Levels) Add(num int, path string) error {
	return l.levels[num].Add(path)
}

type LevelsEdit map[int]LevelEdit

func (l *Levels) Edit(e LevelsEdit) (*Levels, error) {
	levels := &Levels{}

	for idx, level := range l.levels {
		if le, ok := e[idx]; ok {
			cpy, err := level.Edit(le)
			if err != nil {
				return nil, err
			}

			levels.levels = append(levels.levels, cpy)
		} else {
			levels.levels = append(levels.levels, level)
		}
	}

	return levels, nil
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

func (l *Levels) Merge(req MergeRequest) (*Levels, error) {
	if req.Level >= len(l.levels)-1 {
		return nil, ErrTopLevel
	}

	path, rng := l.levels[req.Level].PickRandom()

	merge := NewMerger()

	var origPaths []string

	if req.Level == 0 {
		overlap := l.levels[req.Level].FindOverlap(rng)

		for _, path := range overlap {
			err := merge.Add(path)
			if err != nil {
				return nil, err
			}
		}

		origPaths = overlap
	} else {
		err := merge.Add(path)
		if err != nil {
			return nil, err
		}

		origPaths = []string{path}
	}

	upLevel := req.Level + 1

	up := l.levels[upLevel]

	overlap := up.FindOverlap(rng)

	for _, path := range overlap {
		err := merge.Add(path)
		if err != nil {
			return nil, err
		}
	}

	err := merge.MergeInto(req.File, req.MinVersion)
	if err != nil {
		return nil, err
	}

	return l.Edit(LevelsEdit{
		req.Level: LevelEdit{
			Remove: origPaths,
		},
		upLevel: LevelEdit{
			Remove: overlap,
			Add:    []string{req.File},
		},
	})
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

func (ls *Levels) ConsiderMerges(dir string, ver int64) (*Levels, error) {
	levels := ls

	for idx, l := range ls.levels {
		if l.Size() >= levelMax(idx) {
			file := filepath.Join(dir, fmt.Sprintf("level%d_%d.sst", idx, ver))

			next, err := ls.Merge(
				MergeRequest{
					Level:      idx,
					MinVersion: ver,
					File:       file,
				})
			if err != nil {
				return nil, err
			}

			levels = next
		}
	}

	return levels, nil
}
