package rivetdb

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/evanphx/rivetdb/skiplist"
	"github.com/gogo/protobuf/proto"
)

type WAL struct {
	path string
	f    *os.File
	buf  []byte
}

const (
	WALAppend int32 = 0
	WALCommit int32 = 1
)

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		path: path,
		f:    f,
		buf:  make([]byte, 1024),
	}

	return wal, err
}

func (w *WAL) Close() error {
	w.f.Close()

	return os.Remove(w.path)
}

func (w *WAL) appendEntry(ent *LogEntry) error {
	data, err := ent.Marshal()
	if err != nil {
		return err
	}

	_, err = w.f.Write(data)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(w.buf, uint32(len(data)))

	_, err = w.f.Write(w.buf[:4])
	return err

}

func (w *WAL) Add(ver int64, key, val []byte) error {
	var ent LogEntry

	ent.Op = proto.Int32(WALAppend)
	ent.Version = &ver
	ent.Key = key
	ent.Value = val

	return w.appendEntry(&ent)
}

func (w *WAL) Sync() error {
	return w.f.Sync()
}

func (w *WAL) Commit(ver int64) error {
	var ent LogEntry

	ent.Op = proto.Int32(WALCommit)
	ent.Version = &ver

	err := w.appendEntry(&ent)
	if err != nil {
		return err
	}

	return w.f.Sync()
}

type WALReader struct {
	MaxCommittedVersion int64

	f   *os.File
	buf []byte
}

func NewWALReader(path string) (*WALReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		f:   f,
		buf: make([]byte, 1024),
	}, nil
}

var ErrCorruptWAL = errors.New("corrupt wal file detected")

func (w *WALReader) IntoList() (*skiplist.SkipList, error) {
	list := skiplist.New(0)

	var (
		add bool
		ent LogEntry
	)

	for {
		_, err := w.f.Seek(-4, os.SEEK_CUR)
		if err != nil {
			return nil, err
		}

		_, err = w.f.Read(w.buf[:4])
		if err != nil {
			return nil, err
		}

		entSz := int(binary.BigEndian.Uint32(w.buf))

		_, err = w.f.Seek(int64(-(entSz + 4)), os.SEEK_CUR)
		if err != nil {
			return nil, err
		}

		if len(w.buf) < int(entSz) {
			w.buf = make([]byte, entSz)
		}

		_, err = w.f.Read(w.buf[:entSz])
		if err != nil {
			return nil, err
		}

		err = ent.Unmarshal(w.buf[:entSz])
		if err != nil {
			return nil, err
		}

		switch ent.GetOp() {
		case WALAppend:
			if add {
				list.Set(ent.GetVersion(), ent.Key, ent.Value)
			}
		case WALCommit:
			if !add {
				add = true
				w.MaxCommittedVersion = ent.GetVersion()
			}
		default:
			return nil, ErrCorruptWAL
		}

		pos, err := w.f.Seek(int64(-entSz), os.SEEK_CUR)
		if err != nil {
			return nil, err
		}

		if pos == 0 {
			break
		}
	}

	return list, nil
}
