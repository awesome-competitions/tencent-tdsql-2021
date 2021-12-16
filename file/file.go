package file

import (
    "bytes"
    "io/ioutil"
    "os"
)

type File struct {
    file *os.File
}

func New(path string) (*File, error) {
    file, err := os.OpenFile(path, os.O_RDWR, os.FileMode(0766))
    return &File{
        file: file,
    }, err
}

func (f *File) Name() string{
    return f.file.Name()
}

func (f *File) Read(buff *bytes.Buffer) error {
    _, err := buff.ReadFrom(f.file)
    return err
}

func (f *File) Write(buff *bytes.Buffer) error {
    _, err := buff.WriteTo(f.file)
    return err
}

func (f *File) ReadAll() ([]byte, error){
    return ioutil.ReadAll(f.file)
}

func (f *File) Sync() error{
    return f.file.Sync()
}