package sstable

import "errors"

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

	err := merge.Add(path)
	if err != nil {
		return err
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

	err = merge.MergeInto(req.File, req.MinVersion)
	if err != nil {
		return err
	}

	l.levels[req.Level].Remove(path)

	for _, path := range overlap {
		l.levels[upLevel].Remove(path)
	}

	return l.levels[upLevel].Add(req.File)
}
