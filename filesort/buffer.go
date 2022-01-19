package filesort

import (
	"bytes"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/util"
	"github.com/bits-and-blooms/bloom/v3"
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
	tmp       bytes.Buffer
	tms       bytes.Buffer
	tmk       bytes.Buffer
	upd       int
}

func NewFileBuffer(f *file.File, meta model.Meta) *fileBuffer {
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

func (fb *fileBuffer) Recover(filter *bloom.BloomFilter, t *model.Table, set string, pos int64) error {
	for {
		row, err := fb.NextRow()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if fb.pos == pos {
			break
		}
		if t.DB.Hash()[util.MurmurHash2([]byte(row.ID), 2773)%64] == set {
			filter.AddString(row.Key)
		}
	}
	return nil
}

func (fb *fileBuffer) NextRow() (*model.Row, error) {
	row := model.Row{}
	index := 0
	completed := false
	lastPos := fb.pos
	fb.tmp.Reset()
	fb.tmk.Reset()
	fb.tms.Reset()
	for {
		for ; fb.buf.pos < fb.buf.cap; fb.buf.pos++ {
			b := fb.buf.buf[fb.buf.pos]
			fb.pos++
			if b == consts.COMMA || b == consts.LF {
				s := fb.tmp.String()
				if index == 0 {
					row.ID = s
				}
				if index == fb.upd {
					row.UpdateAt = s
				}
				if fb.tags[index] {
					fb.tmk.WriteString(s)
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
				fb.tmp.Reset()
				if b == consts.LF {
					row.Source = fb.tms.String()
					row.Key = fb.tmk.String()
					completed = true
					fb.buf.pos++
					break
				}
			} else {
				fb.tmp.WriteByte(b)
			}
		}
		if completed {
			break
		}
		capacity, err := fb.f.Read(fb.buf.buf)
		if err != nil {
			if err == io.EOF {
				fb.lastPos = fb.pos
			}
			return nil, err
		}
		fb.readTimes++
		fb.buf.pos = 0
		fb.buf.cap = capacity
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
