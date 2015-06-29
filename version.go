package rivetdb

import "github.com/evanphx/rivetdb/sstable"

type Version struct {
	Id     int64           `json:"id"`
	Tables *sstable.Levels `json:"levels"`
}
