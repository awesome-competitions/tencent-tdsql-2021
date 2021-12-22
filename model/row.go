package model

import (
	"bytes"
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
	"float":    Double,
	"char":     Char,
	"datetime": Datetime,
}

var TypeParser = map[Type]func(str string) interface{}{
	Bigint: func(str string) interface{} {
		v, _ := strconv.ParseInt(str, 10, 64)
		return v
	},
	Double: func(str string) interface{} {
		v, _ := strconv.ParseFloat(str, 64)
		return v
	},
	Float: func(str string) interface{} {
		v, _ := strconv.ParseFloat(str, 64)
		return float32(v)
	},
	Char: func(str string) interface{} {
		return str
	},
	Datetime: func(str string) interface{} {
		return str
	},
}

type Row struct {
	Values   []Value
	Buffer   bytes.Buffer
	Key      string
	UpdateAt string
}

func (r Row) Compare(or interface{}) bool {
	for i, v := range r.Values {
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
			sql += "'" + v.Source + "'"
		} else {
			sql += v.Source
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
	Type   Type
	Value  interface{}
	Source string
}

func (v Value) Equals(o Value) bool {
	return v.Value == o.Value
}

func (v Value) Compare(o Value) int {
	switch v.Type {
	case Bigint:
		if v.Value.(int64) > o.Value.(int64) {
			return 1
		} else if v.Value.(int64) > o.Value.(int64) {
			return 0
		} else {
			return -1
		}
	case Double:
		if v.Value.(float64) > o.Value.(float64) {
			return 1
		} else if v.Value.(float64) > o.Value.(float64) {
			return 0
		} else {
			return -1
		}
	case Float:
		if v.Value.(float32) > o.Value.(float32) {
			return 1
		} else if v.Value.(float32) > o.Value.(float32) {
			return 0
		} else {
			return -1
		}
	case Char, Datetime:
		if v.Value.(string) > o.Value.(string) {
			return 1
		} else if v.Value.(string) > o.Value.(string) {
			return 0
		} else {
			return -1
		}
	}
	return 0
}
