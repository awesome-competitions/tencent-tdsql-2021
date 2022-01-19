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
	err = r.Make(2, 3, 4, 5, 6)
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	err = r.Make(4, 5, 6, 7, 8)
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
}
