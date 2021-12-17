package table

import (
	"github.com/pingcap/parser/mysql"
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

var SqlTypeMapping = map[byte]Type{
	mysql.TypeLonglong: Bigint,
	mysql.TypeDouble:   Double,
	mysql.TypeFloat:    Double,
	mysql.TypeString:   Char,
	mysql.TypeDatetime: Datetime,
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

type Row []Value

func (r Row) Compare(or Row) bool {
	for i, v := range r {
		result := v.Compare(or[i])
		if result != 0 {
			return result > 0
		}
	}
	return false
}

func (r Row) String() string {
	str := ""
	for i, v := range r {
		if v.T == Char || v.T == Datetime {
			str += "'" + v.S + "'"
		} else {
			str += v.S
		}
		if i < len(r)-1 {
			str += ","
		}
	}
	return str
}

type Rows []Row

func (rs *Rows) Len() int {
	return len(*rs) //
}

func (rs *Rows) Less(i, j int) bool {
	return !(*rs)[i].Compare((*rs)[j])
}

func (rs *Rows) Swap(i, j int) {
	(*rs)[i], (*rs)[j] = (*rs)[j], (*rs)[i]
}

type Value struct {
	T Type
	V interface{}
	S string
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
