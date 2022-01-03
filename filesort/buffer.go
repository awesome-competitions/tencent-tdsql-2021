package filesort

import (
	"bytes"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"io"
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
	lastPos   int64
	readTimes int
}

func newFileBuffer(f *file.File, meta model.Meta) *fileBuffer {
	cols := meta.PrimaryKeys
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

func (fb *fileBuffer) Reset(offset int64) {
	_, err := fb.f.Seek(offset, io.SeekStart)
	if err != nil {
		log.Error(err)
	}
	fb.buf.reset()
	fb.pos = offset
	fb.lastPos = offset
}

func (fb *fileBuffer) NextRow() (*model.Row, error) {
	row, err := fb._nextRow()
	if err != nil {
		return nil, err
	}
	if row.Invalid {
		return fb.NextRow()
	}
	return row, nil
}

func (fb *fileBuffer) _nextRow() (*model.Row, error) {
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
	lastPos := fb.pos
	for {
		for ; fb.buf.pos < fb.buf.cap; fb.buf.pos++ {
			b := fb.buf.buf[fb.buf.pos]
			fb.pos++
			row.Buffer.WriteByte(b)
			if b == consts.COMMA || b == consts.LF {
				bs := row.Buffer.Bytes()[start:i]
				start = i + 1
				t := fb.meta.ColsType[fb.meta.Cols[index]]
				s := string(bs)
				if index == upd {
					row.UpdateAt = s
				}
				value, err := model.TypeParser[t](s)
				if err != nil {
					log.Error(err)
					row.Invalid = true
				}
				v := model.Value{
					Type:     t,
					Value:    value,
					Source:   s,
					Sortable: fb.tags[index],
				}
				row.Values[index] = v
				if fb.tags[index] {
					key.WriteString(v.String())
					key.WriteRune(':')
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
	}
	fb.lastPos = lastPos
	row.Key = key.String()
	return &row, nil
}

func (fb *fileBuffer) Delete() {
	_ = fb.f.Delete()
}

func (fb *fileBuffer) Position() int64 {
	return fb.pos
}

func (fb *fileBuffer) LastPosition() int64 {
	return fb.lastPos
}
