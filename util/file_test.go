package util

import (
    "github.com/stretchr/testify/assert"
    "os"
    "testing"
)

func TestParseName(t *testing.T) {
    assert.Equal(t, ParseName("a" + string(os.PathSeparator) + "b"), "b")
    assert.Equal(t, ParseName("a" + string(os.PathSeparator) + "b.jpg"), "b")
}
