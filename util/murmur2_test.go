package util

import "testing"

func TestMurmurHash2(t *testing.T) {
	t.Log(MurmurHash2([]byte("ab"), 2773))
}
