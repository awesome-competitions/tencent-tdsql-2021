package main

import (
	"fmt"
	"strconv"
)

func main() {
	f := float64(96257.35704864987)
	fmt.Println(strconv.FormatFloat(f, 'f', 1, 64))
}
