package rivetdb

import (
	"github.com/evanphx/rivetdb/skiplist"
	"github.com/evanphx/rivetdb/sstable"
)

type Version struct {
	Id     int64           `json:"id"`
	Tables *sstable.Levels `json:"levels"`

	mem *skiplist.SkipList
	imm *skiplist.SkipList
}

func NewVersion(id int64, tbl *sstable.Levels) *Version {
	return &Version{
		Id:     id,
		Tables: tbl,
		mem:    skiplist.New(0),
	}
}

func (v *Version) Ref() {
	v.Tables.Ref()
}

func (v *Version) Discard() {
	v.Tables.Discard()
}

func (v *Version) UpdateTables(tbl *sstable.Levels) *Version {
	cpy := &Version{
		Id:     v.Id,
		Tables: tbl,
		mem:    v.mem,
		imm:    v.imm,
	}

	return cpy
}

func (v *Version) MakeImmutable() *Version {
	return &Version{
		Id:     v.Id,
		Tables: v.Tables,
		mem:    skiplist.New(0),
		imm:    v.mem,
	}
}
