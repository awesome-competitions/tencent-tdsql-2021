package model

import "github.com/ainilili/tdsql-competition/file"

type Table struct {
    Name string
    Database string
    Data []TableData
    Schema *file.File
    Meta TableMeta
}

type TableData struct {
    DataSource string
    Data *file.File
}

type TableMeta struct {
    Keys []string
    Cols []string
}
