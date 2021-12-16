package util

import (
    "bytes"
    "os"
    "strings"
)

func ParseName(path string) string  {
    arr := strings.Split(path, string(os.PathSeparator))
    name := arr[len(arr) - 1]
    if i := strings.Index(name, "."); i != -1 {
        name = name[:i]
    }
    return name
}

func AssemblePath(names ...string) string{
    buf := bytes.Buffer{}
    for i, name := range names{
        buf.WriteString(name)
        if i < len(names) - 1 {
            buf.WriteRune(os.PathSeparator)
        }
    }
    return buf.String()
}