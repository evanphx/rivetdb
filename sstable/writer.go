package sstable

import (
	"bufio"

	"github.com/gogo/protobuf/proto"
)
import "encoding/binary"
import "os"

type idxkey struct {
	key []byte
	off uint64
}

type Writer struct {
	path string
	f    *os.File
	bw   *bufio.Writer

	pos  uint64
	keys []idxkey

	buf []byte
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(1024, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		path: path,
		f:    f,
		bw:   bufio.NewWriter(f),
		pos:  1024,
		keys: make([]idxkey, 0, 1000),
		buf:  make([]byte, 1024),
	}

	return w, nil
}

func (w *Writer) Add(key, value []byte) error {
	var entry Entry

	entry.Key = key
	entry.Value = value

	entSz := entry.Size()

	sz := binary.PutUvarint(w.buf, uint64(entSz))

	_, err := w.bw.Write(w.buf[:sz])
	if err != nil {
		return err
	}

	data, err := entry.Marshal()
	if err != nil {
		return err
	}

	_, err = w.bw.Write(data)
	if err != nil {
		return err
	}

	w.keys = append(w.keys, idxkey{key, w.pos})

	w.pos += uint64(sz + entSz)

	return nil
}

func (w *Writer) Close() error {
	var hdr FileHeader

	hdr.Keys = proto.Uint64(uint64(len(w.keys)))
	hdr.Index = proto.Uint64(w.pos)

	b := make([]byte, 1024)

	var tocSz uint64

	for _, i := range w.keys {
		var ie IndexEntry

		ie.Key = i.key
		ie.Offset = &i.off

		ieSz := ie.Size()

		if len(b) < ieSz {
			b = make([]byte, ieSz)
		}

		sz := binary.PutUvarint(b, uint64(ieSz))

		tocSz += uint64(sz)

		_, err := w.bw.Write(b[:sz])
		if err != nil {
			return err
		}

		sz, err = ie.MarshalTo(b)
		if err != nil {
			return err
		}

		tocSz += uint64(sz)

		_, err = w.bw.Write(b[:sz])
		if err != nil {
			return err
		}
	}

	w.bw.Flush()

	hdr.IndexSize = &tocSz

	w.f.Seek(0, os.SEEK_SET)

	data, err := hdr.Marshal()
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint16(w.buf, uint16(len(data)))

	_, err = w.f.Write(w.buf[:2])
	if err != nil {
		return err
	}

	_, err = w.f.Write(data)
	return err
}
