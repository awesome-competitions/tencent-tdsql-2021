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
	tags      map[int]bool
	pos       int64
	readTimes int
}

func newFileBuffer(f *file.File, meta model.Meta) *fileBuffer {
	cols := meta.Keys
	if len(cols) == 0 {
		cols = meta.Cols
	}
	tags := map[int]bool{}
	for _, col := range cols {
		if col != "updated_at" {
			tags[meta.ColsIndex[col]] = true
		}
	}
	return &fileBuffer{
		buf: &buffer{
			buf: make([]byte, consts.FileBufferSize),
		},
		f:    f,
		meta: meta,
		tags: tags,
	}
}

func (fb *fileBuffer) Jump(c int) error {
	if c == 0 {
		return nil
	}
	i := 0
	eof := false
	for {
		for ; fb.buf.pos < fb.buf.cap; fb.buf.pos++ {
			b := fb.buf.buf[fb.buf.pos]
			if b == consts.LF {
				i++
				if i >= c {
					eof = true
					fb.buf.pos++
					break
				}
			}
		}
		if eof {
			break
		}
		capacity, err := fb.f.Read(fb.buf.buf)
		if err != nil {
			return err
		}
		fb.readTimes++
		fb.buf.pos = 0
		fb.buf.cap = capacity
		fb.pos += int64(capacity)
	}
	return nil
}

func (fb *fileBuffer) NextRow() (*model.Row, error) {
	row := model.Row{
		Values: make([]model.Value, len(fb.meta.Cols)),
		Buffer: bytes.Buffer{},
	}
	i := 0
	index := 0
	start := 0
	eof := false
	upd := fb.meta.ColsIndex["updated_at"]
	key := bytes.Buffer{}
	for {
		for ; fb.buf.pos < fb.buf.cap; fb.buf.pos++ {
			b := fb.buf.buf[fb.buf.pos]
			row.Buffer.WriteByte(b)
			if b == consts.COMMA || b == consts.LF {
				bs := row.Buffer.Bytes()[start:i]
				start = i + 1
				t := fb.meta.ColsType[fb.meta.Cols[index]]
				s := string(bs)
				if fb.tags[index] {
					key.Write(bs)
					key.WriteRune(':')
				}
				if index == upd {
					row.UpdateAt = s
				}
				row.Values[index] = model.Value{
					Type:   t,
					Value:  model.TypeParser[t](*(*string)(unsafe.Pointer(&bs))),
					Source: s,
				}
				index++
				if b == consts.LF {
					eof = true
					fb.buf.pos++
					break
				}
			}
			i++
		}
		if eof {
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
	row.Key = key.String()
	return &row, nil
}

func (fb *fileBuffer) Delete() {
	_ = fb.f.Delete()
}
