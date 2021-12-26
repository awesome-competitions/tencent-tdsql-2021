package model

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
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
		if len(str) > 32 {
			return str, errors.New(fmt.Sprintf("char len err: %d", len(str)))
		}
		return str, nil
	},
	Datetime: func(str string) (interface{}, error) {
		if len(str) != 19 {
			return str, errors.New(fmt.Sprintf("datetime len err: %d", len(str)))
		}
		return str, nil
	},
}

type Row struct {
	Values   []Value
	Buffer   bytes.Buffer
	Key      string
	UpdateAt string
	Invalid  bool
}

func (r Row) Compare(or interface{}) bool {
	for i, v := range r.Values {
		if !v.Sortable {
			continue
		}
		result := v.Compare(or.(Row).Values[i])
		if result != 0 {
			return result > 0
		}
	}
	return false
}

func (r Row) String() string {
	sql := ""
	for i, v := range r.Values {
		if v.Type.IsString() {
			sql += "'" + v.String() + "'"
		} else {
			sql += v.String()
		}
		if i < len(r.Values)-1 {
			sql += ","
		}
	}
	return sql
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
