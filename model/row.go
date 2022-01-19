package model

import (
	"strconv"
	"strings"
)

type Type int

const (
	_ Type = iota
	Bigint
	Double
	Float
	Char
	Datetime
)

func (t Type) IsString() bool {
	return t == Char || t == Datetime
}

var SqlTypeMapping = map[string]Type{
	"bigint":   Bigint,
	"double":   Double,
	"float":    Float,
	"char":     Char,
	"datetime": Datetime,
}

var TypeParser = map[Type]func(str string) (interface{}, error){
	Bigint: func(str string) (interface{}, error) {
		return strconv.ParseInt(str, 10, 64)
	},
	Double: func(str string) (interface{}, error) {
		return strconv.ParseFloat(str, 64)
	},
	Float: func(str string) (interface{}, error) {
		return strconv.ParseFloat(str, 64)
	},
	Char: func(str string) (interface{}, error) {
		return str, nil
	},
	Datetime: func(str string) (interface{}, error) {
		return str, nil
	},
}

type Row struct {
	Key    string
	Source string
	SortID int
}

func (r Row) Compare(or interface{}) bool {
	if r.SortID == or.(Row).SortID {
		return r.Key > or.(Row).Key
	}
	return r.SortID > or.(Row).SortID
}

func (r Row) String() string {
	return r.Source
}

func (r Row) ID() string {
	return r.Source[0:strings.Index(r.Source, ",")]
}

func (r Row) UpdateAt() string {
	return r.Source[strings.LastIndex(r.Source, ",")+1:]
}

type Rows []*Row

func (rs *Rows) Len() int {
	return len(*rs) //
}

func (rs *Rows) Less(i, j int) bool {
	return !(*rs)[i].Compare(*(*rs)[j])
}

func (rs *Rows) Swap(i, j int) {
	(*rs)[i], (*rs)[j] = (*rs)[j], (*rs)[i]
}

type Value struct {
	Type     Type
	Value    interface{}
	Source   string
	Sortable bool
}

func (v Value) String() string {
	//if v.Type == Char {
	//	return v.Source[2:]
	//}
	return v.Source
}

func (v Value) Equals(o Value) bool {
	return v.Value == o.Value
}

func (v Value) Compare(o Value) int {
	switch v.Type {
	case Bigint:
		if v.Value.(int64) > o.Value.(int64) {
			return 1
		} else if v.String() == o.String() {
			return 0
		} else {
			return -1
		}
	case Double:
		if v.Value.(float64) > o.Value.(float64) {
			return 1
		} else if v.String() == o.String() {
			return 0
		} else {
			return -1
		}
	case Float:
		if v.Value.(float64) > o.Value.(float64) {
			return 1
		} else if v.String() == o.String() {
			return 0
		} else {
			return -1
		}
	case Char, Datetime:
		if v.Value.(string) > o.Value.(string) {
			return 1
		} else if v.String() == o.String() {
			return 0
		} else {
			return -1
		}
	}
	return 0
}
