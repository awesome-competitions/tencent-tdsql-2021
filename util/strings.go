package util

import "strconv"

func JoinInt64(is []int64, sp string) string {
	r := ""
	for _, i := range is {
		r += strconv.FormatInt(i, 10) + sp
	}
	return r[:len(r)-1]
}
