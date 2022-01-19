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

func (r *Recover) Make(flag int, pos int64, total int64, lastPos int64, lastTotal int64) error {
	flagBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(flagBytes, uint32(flag))
	posBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(posBytes, uint64(pos))
	totalBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(totalBytes, uint64(total))
	lastPosBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastPosBytes, uint64(lastPos))
	lastTotalBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastTotalBytes, uint64(lastTotal))
	buf := bytes.Buffer{}
	buf.Write(flagBytes)
	buf.Write(posBytes)
	buf.Write(totalBytes)
	buf.Write(lastPosBytes)
	buf.Write(lastTotalBytes)
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

func (r *Recover) Load() (int, int64, int64, int64, int64, error) {
	_, err := r.file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	bs, err := r.file.ReadAll()
	if err != nil {
		if err == io.EOF {
			return 0, 0, 0, 0, 0, nil
		}
		return 0, 0, 0, 0, 0, err
	}
	if len(bs) == 0 {
		return 0, 0, 0, 0, 0, nil
	}
	return int(binary.BigEndian.Uint32(bs[:4])),
		int64(binary.BigEndian.Uint64(bs[4:12])),
		int64(binary.BigEndian.Uint64(bs[12:20])),
		int64(binary.BigEndian.Uint64(bs[20:28])),
		int64(binary.BigEndian.Uint64(bs[28:36])),
		nil
}
