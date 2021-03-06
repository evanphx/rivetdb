// Code generated by protoc-gen-gogo.
// source: db.proto
// DO NOT EDIT!

/*
	Package rivetdb is a generated protocol buffer package.

	It is generated from these files:
		db.proto

	It has these top-level messages:
		LogEntry
*/
package rivetdb

import proto "github.com/gogo/protobuf/proto"
import math "math"

// discarding unused import gogoproto "github.com/gogo/protobuf/gogoproto/gogo.pb"

import io "io"
import fmt "fmt"

import bytes "bytes"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type LogEntry struct {
	Op               *int32 `protobuf:"varint,1,opt,name=op" json:"op,omitempty"`
	Key              []byte `protobuf:"bytes,2,opt,name=key" json:"key,omitempty"`
	Value            []byte `protobuf:"bytes,3,opt,name=value" json:"value,omitempty"`
	Version          *int64 `protobuf:"varint,4,opt,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *LogEntry) Reset()         { *m = LogEntry{} }
func (m *LogEntry) String() string { return proto.CompactTextString(m) }
func (*LogEntry) ProtoMessage()    {}

func (m *LogEntry) GetOp() int32 {
	if m != nil && m.Op != nil {
		return *m.Op
	}
	return 0
}

func (m *LogEntry) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *LogEntry) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *LogEntry) GetVersion() int64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

func init() {
}
func (m *LogEntry) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Op", wireType)
			}
			var v int32
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Op = &v
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append([]byte{}, data[index:postIndex]...)
			index = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = append([]byte{}, data[index:postIndex]...)
			index = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			var v int64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Version = &v
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := skipDb(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}

	return nil
}
func skipDb(data []byte) (n int, err error) {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for {
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				index++
				if data[index-1] < 0x80 {
					break
				}
			}
			return index, nil
		case 1:
			index += 8
			return index, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			index += length
			return index, nil
		case 3:
			for {
				var wire uint64
				var start int = index
				for shift := uint(0); ; shift += 7 {
					if index >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[index]
					index++
					wire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				wireType := int(wire & 0x7)
				if wireType == 4 {
					break
				}
				next, err := skipDb(data[start:])
				if err != nil {
					return 0, err
				}
				index = start + next
			}
			return index, nil
		case 4:
			return index, nil
		case 5:
			index += 4
			return index, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}
func (m *LogEntry) Size() (n int) {
	var l int
	_ = l
	if m.Op != nil {
		n += 1 + sovDb(uint64(*m.Op))
	}
	if m.Key != nil {
		l = len(m.Key)
		n += 1 + l + sovDb(uint64(l))
	}
	if m.Value != nil {
		l = len(m.Value)
		n += 1 + l + sovDb(uint64(l))
	}
	if m.Version != nil {
		n += 1 + sovDb(uint64(*m.Version))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovDb(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozDb(x uint64) (n int) {
	return sovDb(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *LogEntry) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *LogEntry) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Op != nil {
		data[i] = 0x8
		i++
		i = encodeVarintDb(data, i, uint64(*m.Op))
	}
	if m.Key != nil {
		data[i] = 0x12
		i++
		i = encodeVarintDb(data, i, uint64(len(m.Key)))
		i += copy(data[i:], m.Key)
	}
	if m.Value != nil {
		data[i] = 0x1a
		i++
		i = encodeVarintDb(data, i, uint64(len(m.Value)))
		i += copy(data[i:], m.Value)
	}
	if m.Version != nil {
		data[i] = 0x20
		i++
		i = encodeVarintDb(data, i, uint64(*m.Version))
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeFixed64Db(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Db(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintDb(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
func (this *LogEntry) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*LogEntry)
	if !ok {
		return false
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if this.Op != nil && that1.Op != nil {
		if *this.Op != *that1.Op {
			return false
		}
	} else if this.Op != nil {
		return false
	} else if that1.Op != nil {
		return false
	}
	if !bytes.Equal(this.Key, that1.Key) {
		return false
	}
	if !bytes.Equal(this.Value, that1.Value) {
		return false
	}
	if this.Version != nil && that1.Version != nil {
		if *this.Version != *that1.Version {
			return false
		}
	} else if this.Version != nil {
		return false
	} else if that1.Version != nil {
		return false
	}
	if !bytes.Equal(this.XXX_unrecognized, that1.XXX_unrecognized) {
		return false
	}
	return true
}
