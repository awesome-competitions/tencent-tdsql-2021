package model

import "github.com/ainilili/tdsql-competition/filesort"

type Sql struct {
	Sql      string
	Record   string
	Finished bool
}

type Task struct {
	Fs  *filesort.FileSorter
	Set string
}
