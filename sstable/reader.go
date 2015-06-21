package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type Reader struct {
	path string
	f    *os.File

	hdr     FileHeader
	indexes []*IndexEntry

	Start, End []byte
}

func NewReader(path string) (*Reader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		path: path,
		f:    f,
	}

	err = r.init()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reader) readIndex(data []byte) (*IndexEntry, error) {
	var ie IndexEntry

	entSz, sz := binary.Uvarint(data)

	data = data[sz:]

	err := ie.Unmarshal(data[:entSz])
	if err != nil {
		return nil, err
	}

	return &ie, nil
}

func (r *Reader) init() error {
	buf := make([]byte, 1024)

	_, err := io.ReadFull(r.f, buf)
	if err != nil {
		return err
	}

	hsz := binary.BigEndian.Uint16(buf)

	err = r.hdr.Unmarshal(buf[2 : 2+hsz])
	if err != nil {
		return err
	}

	idx := r.hdr.GetIndex()

	_, err = r.f.Seek(int64(idx), os.SEEK_SET)
	if err != nil {
		return err
	}

	idxData := make([]byte, r.hdr.GetIndexSize())

	_, err = io.ReadFull(r.f, idxData)
	if err != nil {
		return err
	}

	data := idxData

	for len(data) > 0 {
		var ie IndexEntry

		entSz, sz := binary.Uvarint(data)

		data = data[sz:]

		err = ie.Unmarshal(data[:entSz])
		if err != nil {
			return err
		}

		data = data[entSz:]

		r.indexes = append(r.indexes, &ie)
	}

	// Read last and first

	ie, err := r.readIndex(idxData)
	if err != nil {
		return err
	}

	r.Start = ie.Key

	ie, err = r.readIndex(idxData[r.hdr.GetLastIndex():])
	if err != nil {
		return err
	}

	r.End = ie.Key

	return nil
}

func (r *Reader) Get(ver int64, key []byte) ([]byte, error) {
	for _, idx := range r.indexes {
		if idx.GetVersion() <= ver && bytes.Equal(idx.Key, key) {
			entry, err := r.readEntry(int64(idx.GetOffset()))
			if err != nil {
				return nil, err
			}

			return entry.Value, nil
		}
	}

	return nil, nil
}

func (r *Reader) readEntry(off int64) (*Entry, error) {
	_, err := r.f.Seek(off, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	br := bufio.NewReader(r.f)

	entSz, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}

	ebuf := make([]byte, entSz)

	_, err = br.Read(ebuf)
	if err != nil {
		return nil, err
	}

	var entry Entry

	err = entry.Unmarshal(ebuf)
	if err != nil {
		return nil, err
	}

	return &entry, nil
}
