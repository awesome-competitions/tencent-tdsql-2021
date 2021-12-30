package model

import "bytes"

type SqlBuffer struct {
	Buff   bytes.Buffer
	Offset int
	Index  int
	Set    string
}

func NewSqlBuffer(offset int, set string) *SqlBuffer {
	return &SqlBuffer{
		Buff:   bytes.Buffer{},
		Offset: offset,
		Set:    set,
	}
}
