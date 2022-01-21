package filesort

import (
	"bytes"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"io"
	"strconv"
)

type buffer struct {
	buf []byte
	pos int
	cap int
	eof bool
}

func (bf *buffer) reset() {
	bf.pos = 0
	bf.cap = 0
	bf.eof = false
}

type fileBuffer struct {
	buf       *buffer
	f         *file.File
	meta      model.Meta
	tags      map[int]bool
	pos       int64
	lastPos   int64
	readTimes int
	tmp       bytes.Buffer
	tms       bytes.Buffer
	tmk       bytes.Buffer
	upd       int
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
	upd := meta.ColsIndex["updated_at"]
	return &fileBuffer{
		buf: &buffer{
			buf: make([]byte, consts.FileBufferSize),
		},
		f:    f,
		meta: meta,
		tags: tags,
		tmp:  bytes.Buffer{},
		tms:  bytes.Buffer{},
		tmk:  bytes.Buffer{},
		upd:  upd,
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
	row := model.Row{}
	buf := fb.buf
	dif := buf.cap - buf.pos
	if dif < 100 && !buf.eof {
		copy(buf.buf[:dif], buf.buf[buf.pos:buf.cap])
		capacity, err := fb.f.Read(buf.buf[dif:])
		if err != nil {
			if err == io.EOF {
				buf.eof = true
			}
		}
		fb.readTimes++
		fb.buf.pos = 0
		fb.buf.cap = dif + capacity
	}
	start := buf.pos
	index := 0
	fb.tmk.Reset()
	fb.tms.Reset()
	lastPos := fb.pos
	for ; buf.pos < buf.cap; buf.pos++ {
		b := buf.buf[buf.pos]
		fb.pos++
		if b == consts.LF || b == consts.COMMA {
			s := string(buf.buf[start:buf.pos])
			if index == 0 {
				row.SortID, _ = strconv.Atoi(s)
			}
			if fb.tags[index] {
				fb.tmk.WriteString(s)
				fb.tmk.WriteByte(',')
			}
			t := fb.meta.ColsType[fb.meta.Cols[index]]
			if t.IsString() && s[0] != '\'' {
				fb.tms.WriteByte('\'')
				fb.tms.WriteString(s)
				fb.tms.WriteByte('\'')
			} else {
				fb.tms.WriteString(s)
			}
			if b == consts.COMMA {
				fb.tms.WriteByte(consts.COMMA)
			}
			index++
			start = buf.pos + 1
			if b == consts.LF {
				row.Source = fb.tms.String()
				row.Key = fb.tmk.String()
				fb.buf.pos++
				break
			}
		}
	}
	if row.Source == "" {
		fb.lastPos = fb.pos
		return nil, io.EOF
	}
	fb.lastPos = lastPos
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
