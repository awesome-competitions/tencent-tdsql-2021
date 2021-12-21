package model

import (
	"bytes"
	"strconv"
	"unsafe"
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
	Values []Value
	Buffer bytes.Buffer
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
	return *(*string)(unsafe.Pointer(&r.Buffer))
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
	T Type
	V interface{}
	S string
}

func (v Value) Equals(o Value) bool {
	return v.S == o.S
}

func (v Value) Compare(o Value) int {
	switch v.T {
	case Bigint:
		if v.V.(int64) > o.V.(int64) {
			return 1
		} else if v.V.(int64) > o.V.(int64) {
			return 0
		} else {
			return -1
		}
	case Double:
		if v.V.(float64) > o.V.(float64) {
			return 1
		} else if v.V.(float64) > o.V.(float64) {
			return 0
		} else {
			return -1
		}
	case Float:
		if v.V.(float32) > o.V.(float32) {
			return 1
		} else if v.V.(float32) > o.V.(float32) {
			return 0
		} else {
			return -1
		}
	case Char, Datetime:
		if v.V.(string) > o.V.(string) {
			return 1
		} else if v.V.(string) > o.V.(string) {
			return 0
		} else {
			return -1
		}
	}
	return 0
}
