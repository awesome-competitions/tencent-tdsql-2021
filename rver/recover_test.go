package rver

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestRecover_Make(t *testing.T) {
	r, err := New("test")
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	err = r.Make(2, "abc")
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
	err = r.Make(4, "")
	if err != nil {
		log.Fatal(err)
	}
	t.Log(r.Load())
}

func TestRecover_Make2(t *testing.T) {
	r, _ := New("test")
	s := time.Now().UnixNano()
	for i := 0; i < 4480; i++ {
		r.Make(124, "12314,13214564,4564564")
	}
	fmt.Println((time.Now().UnixNano() - s) / 1e6)
}
