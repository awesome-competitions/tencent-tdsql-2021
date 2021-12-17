package file

import (
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

func (f *File) ReadAll() ([]byte, error){
    return ioutil.ReadAll(f.file)
}

func (f *File) Sync() error{
    return f.file.Sync()
}