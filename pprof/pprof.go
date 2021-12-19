package pprof

import (
	"net/http"
	_ "net/http/pprof"
)

func StartPprofServer() error {
	return http.ListenAndServe("0.0.0.0:8000", nil)
}
