package rver

import (
	"log"
	"testing"
)

func TestRecover_Make(t *testing.T) {
	r, err := New("test")
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	t.Log(r.RowIndex)
	err = r.Make(2)
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	t.Log(r.RowIndex)
	err = r.Make(-1)
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	t.Log(r.RowIndex)
}
