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
}

type Source struct {
	DataSource string
	File       *file.File
}

type Meta struct {
	Keys      []string
	Cols      []string
	ColsIndex map[string]int
	ColsType  map[string]Type
}
