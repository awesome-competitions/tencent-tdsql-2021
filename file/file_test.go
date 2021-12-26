package file

import (
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	f, err := New("test", os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	t.Log(err)
	t.Log(f)
}
