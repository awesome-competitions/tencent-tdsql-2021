package rver

import (
	"bytes"
	"encoding/binary"
	"github.com/ainilili/tdsql-competition/file"
	"io"
	"os"
)

type Recover struct {
	file *file.File
}

func New(path string) (*Recover, error) {
	f, err := file.New(path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return &Recover{file: f}, nil
}

func (r *Recover) Make(flag int, path string) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(flag))
	buf := bytes.Buffer{}
	buf.Write(data)
	buf.WriteString(path)
	err := r.file.Truncate(0)
	if err != nil {
		return err
	}
	err = r.file.WriteAt(0, buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (r *Recover) Load() (int, string, error) {
	_, err := r.file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, "", err
	}
	bs, err := r.file.ReadAll()
	if err != nil {
		if err == io.EOF {
			return 0, "", nil
		}
		return 0, "", err
	}
	if len(bs) == 0 {
		return 0, "", nil
	}
	return int(binary.BigEndian.Uint32(bs[:4])), string(bs[4:]), nil
}
