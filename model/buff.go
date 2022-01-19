package model

import "bytes"

type Buffer struct {
	Buffer     *bytes.Buffer
	BufferSize int
	HeaderSize int
}

func (b *Buffer) Reset() {
	b.BufferSize = 0
	b.Buffer.Truncate(b.HeaderSize)
}

type Query struct {
	Sql      []string
	Pos      int64
	Finished bool
}
