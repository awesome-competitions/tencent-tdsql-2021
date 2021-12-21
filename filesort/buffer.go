package filesort

import (
	"bytes"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/model"
	"unsafe"
)

type buffer struct {
	buf []byte
	pos int
	cap int
}

func (bf *buffer) reset() {
	bf.pos = 0
	bf.cap = 0
}

type fileBuffer struct {
	buf       *buffer
	f         *file.File
	meta      model.Meta
	pos       int64
	readTimes int
}

func newFileBuffer(f *file.File, meta model.Meta) *fileBuffer {
	return &fileBuffer{
		buf: &buffer{
			buf: make([]byte, consts.FileBufferSize),
		},
		f:    f,
		meta: meta,
	}
}

func (fb *fileBuffer) nextRow() (*model.Row, error) {
	row := model.Row{
		Values: make([]model.Value, len(fb.meta.Cols)),
		Buffer: bytes.Buffer{},
	}
	i := 0
	index := 0
	start := 0
	end := false
	for {
		for ; fb.buf.pos < fb.buf.cap; fb.buf.pos++ {
			b := fb.buf.buf[fb.buf.pos]
			row.Buffer.WriteByte(b)
			if b == consts.LF {
				end = true
				fb.buf.pos++
				break
			}
			if b == consts.COMMA {
				bs := row.Buffer.Bytes()[start:i]
				s := *(*string)(unsafe.Pointer(&bs))
				start = i + 1
				t := fb.meta.ColsType[fb.meta.Cols[index]]
				row.Values[index] = model.Value{
					T: t,
					V: model.TypeParser[t](s),
					S: s,
				}
				index++
			}
			i++
		}
		if end {
			break
		}
		capacity, err := fb.f.Read(fb.buf.buf)
		if err != nil {
			return nil, err
		}
		fb.readTimes++
		fb.buf.pos = 0
		fb.buf.cap = capacity
		fb.pos += int64(capacity)
	}
	return &row, nil
}
