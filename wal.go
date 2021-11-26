package resumefile

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Operator uint32

const (
	WAL_PUT    Operator = 10 // Put 操作
	WAL_REMOVE Operator = 20 // Remove 操作
)
const wlen = 20

var walSuffix = ".rf"

type Wal struct {
	Op  Operator
	Key PartRange
}

func (wal *Wal) Encode() []byte {
	return WalEncode(wal.Op, &wal.Key)
}

func NewWalDecode(buf []byte) *Wal {

	wal := &Wal{}
	wal.Op = Operator(binary.BigEndian.Uint32(buf[0:4]))
	wal.Key.Start = binary.BigEndian.Uint64(buf[4:12])
	wal.Key.End = binary.BigEndian.Uint64(buf[12:20])

	return wal
}

func NewWalsDecode(buf []byte) ([]*Wal, error) {
	var result []*Wal

	if len(buf)%wlen != 0 {
		return nil, fmt.Errorf("buf len is error")
	}

	for i := 0; i < len(buf); i += wlen {
		result = append(result, NewWalDecode(buf[i:i+wlen]))
	}

	return result, nil
}

func WalEncode(Op Operator, Key *PartRange) []byte {
	var buf = bytes.NewBuffer(nil)
	var err error
	err = binary.Write(buf, binary.BigEndian, Op)
	if err != nil {
		panic(err)
	}
	err = binary.Write(buf, binary.BigEndian, Key.Start)
	if err != nil {
		panic(err)
	}
	err = binary.Write(buf, binary.BigEndian, Key.End)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}
