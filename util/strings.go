package util

import (
	"encoding/json"
	"github.com/shopspring/decimal"
	"strconv"
)

func JoinInt64(is []int64, sp string) string {
	r := ""
	for _, i := range is {
		r += strconv.FormatInt(i, 10) + sp
	}
	return r[:len(r)-1]
}

func String(dest interface{}) string {
	var key string
	if dest == nil {
		return key
	}
	switch dest.(type) {
	case float64:
		key = decimal.NewFromFloat(dest.(float64)).String()
	case *float64:
		key = decimal.NewFromFloat(*dest.(*float64)).String()
	case float32:
		key = decimal.NewFromFloat32(dest.(float32)).String()
	case *float32:
		key = decimal.NewFromFloat32(*dest.(*float32)).String()
	case int:
		key = strconv.Itoa(dest.(int))
	case *int:
		key = strconv.Itoa(*dest.(*int))
	case uint:
		key = strconv.Itoa(int(dest.(uint)))
	case *uint:
		key = strconv.Itoa(int(*dest.(*uint)))
	case int8:
		key = strconv.Itoa(int(dest.(int8)))
	case *int8:
		key = strconv.Itoa(int(*dest.(*int8)))
	case uint8:
		key = strconv.Itoa(int(dest.(uint8)))
	case *uint8:
		key = strconv.Itoa(int(*dest.(*uint8)))
	case int16:
		key = strconv.Itoa(int(dest.(int16)))
	case *int16:
		key = strconv.Itoa(int(*dest.(*int16)))
	case uint16:
		key = strconv.Itoa(int(dest.(uint16)))
	case *uint16:
		key = strconv.Itoa(int(*dest.(*uint16)))
	case int32:
		key = strconv.Itoa(int(dest.(int32)))
	case *int32:
		key = strconv.Itoa(int(*dest.(*int32)))
	case uint32:
		key = strconv.Itoa(int(dest.(uint32)))
	case *uint32:
		key = strconv.Itoa(int(*dest.(*uint32)))
	case int64:
		key = strconv.FormatInt(dest.(int64), 10)
	case *int64:
		key = strconv.FormatInt(*dest.(*int64), 10)
	case uint64:
		key = strconv.FormatUint(dest.(uint64), 10)
	case *uint64:
		key = strconv.FormatUint(*dest.(*uint64), 10)
	case string:
		key = dest.(string)
	case *string:
		key = *dest.(*string)
	case []byte:
		key = string(dest.([]byte))
	case *[]byte:
		key = string(*dest.(*[]byte))
	case bool:
		if dest.(bool) {
			key = "true"
		} else {
			key = "false"
		}
	case *bool:
		if *dest.(*bool) {
			key = "true"
		} else {
			key = "false"
		}
	default:
		newValue, _ := json.Marshal(dest)
		key = string(newValue)
	}
	return key
}
