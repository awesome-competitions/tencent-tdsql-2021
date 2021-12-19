package table

import (
	"fmt"
	"sort"
	"testing"
)

func TestRows_Push(t *testing.T) {
	rows := Rows{}
	rows = append(rows, Row{Value{T: Bigint, V: int64(1), S: "1"}})
	rows = append(rows, Row{Value{T: Bigint, V: int64(3), S: "3"}})
	rows = append(rows, Row{Value{T: Bigint, V: int64(2), S: "2"}})
	sort.Sort(&rows)
	for _, row := range rows {
		fmt.Println(row)
	}
}
