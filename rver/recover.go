package rver

import (
	"encoding/binary"
	"github.com/ainilili/tdsql-competition/file"
	"io"
	"os"
)

type Recover struct {
	file     *file.File
	RowIndex int32
}

func New(path string) (*Recover, error) {
	f, err := file.New(path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return &Recover{file: f}, nil
}

func (r *Recover) Make(rowIndex int32) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(rowIndex))
	err := r.file.WriteAt(0, data)
	if err != nil {
		return err
	}
	r.RowIndex = rowIndex
	return nil
}

func (r *Recover) Load() error {
	data := make([]byte, 4)
	err := r.file.ReadAt(0, data)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	r.RowIndex = int32(binary.BigEndian.Uint32(data))
	return nil
}
