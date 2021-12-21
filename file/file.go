package file

import (
	"io/ioutil"
	"os"
)

type File struct {
	file *os.File
}

func New(path string, flag int) (*File, error) {
	file, err := os.OpenFile(path, flag, os.FileMode(0766))
	return &File{
		file: file,
	}, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *File) Name() string {
	return f.file.Name()
}

func (f *File) WriteAt(offset int64, bytes []byte) error {
	_, err := f.file.WriteAt(bytes, offset)
	return err
}

func (f *File) Write(bytes []byte) (int, error) {
	return f.file.Write(bytes)
}

func (f *File) ReadAt(offset int64, bytes []byte) error {
	_, err := f.file.ReadAt(bytes, offset)
	return err
}

func (f *File) Read(bytes []byte) (int, error) {
	return f.file.Read(bytes)
}

func (f *File) ReadAll() ([]byte, error) {
	return ioutil.ReadAll(f.file)
}

func (f *File) Sync() error {
	return f.file.Sync()
}

func (f *File) Size() int64 {
	info, err := f.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
