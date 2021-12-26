package model

import (
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/rver"
)

type Table struct {
	ID       int
	Name     string
	Database string
	Sources  []Source
	Schema   string
	Meta     Meta
	DB       *database.DB
	Recover  *rver.Recover

	cols string
}

func (t Table) String() string {
	return t.Database + "." + t.Name
}

func (t Table) Cols() string {
	if t.cols != "" {
		return t.cols
	}
	for i, c := range t.Meta.Cols {
		t.cols += c
		if i != len(t.Meta.Cols)-1 {
			t.cols += ","
		}
	}
	return t.cols
}

type Source struct {
	DataSource string
	File       *file.File
}

type Meta struct {
	PrimaryKeys  []string
	Keys         []string
	Cols         []string
	ColsIndex    map[string]int
	ColsType     map[string]Type
	DefaultValue map[string]string
}
