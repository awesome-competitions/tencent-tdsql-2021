package model

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestRows_Push(t *testing.T) {
	rows := Rows{}
	heap.Push(&rows, Row{Values: []Value{{Type: Bigint, Value: int64(1)}}})
	heap.Push(&rows, Row{Values: []Value{{Type: Bigint, Value: int64(3)}}})
	heap.Push(&rows, Row{Values: []Value{{Type: Bigint, Value: int64(2)}}})
	for _, row := range rows {
		fmt.Println(row.Values[0].Value)
	}
	fmt.Println("=========>")
	for rows.Len() > 0 {
		row := heap.Pop(&rows)
		fmt.Println(row.(Row).Values[0].Value)
	}

}
