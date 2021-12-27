package filesort

import "testing"

type testLoserValue struct {
	k int
	v int
}

func (t testLoserValue) Compare(o interface{}) bool {
	return t.k > o.(testLoserValue).k
}

func TestLoserTree(t *testing.T) {
	loser1 := &loser{value: testLoserValue{k: 1, v: 1}}
	loser2 := &loser{value: testLoserValue{k: 3, v: 3}}
	loser3 := &loser{value: testLoserValue{k: 5, v: 5}}
	loser4 := &loser{value: testLoserValue{k: 4, v: 4}}
	loser5 := &loser{value: testLoserValue{k: 7, v: 7}}
	losers := []*loser{loser1, loser2, loser3, loser4, loser5}

	lt := newLoserTree(losers)
	t.Log(lt.root().value)

	loser1.value = testLoserValue{k: 2, v: 2}
	loser1.contest()
	t.Log(lt.root().value)

	loser1.value = testLoserValue{k: 8, v: 8}
	loser1.contest()
	t.Log(lt.root().value)

	loser2.value = testLoserValue{k: 9, v: 9}
	loser2.contest()
	t.Log(lt.root().value)
}

func TestLoserTree1(t *testing.T) {
	loser1 := &loser{value: testLoserValue{k: 1, v: 1}}
	loser2 := &loser{value: testLoserValue{k: 3, v: 3}}
	loser3 := &loser{value: testLoserValue{k: 5, v: 5}}
	loser4 := &loser{value: testLoserValue{k: 4, v: 4}}
	loser5 := &loser{value: testLoserValue{k: 7, v: 7}}
	losers := []*loser{loser1, loser2, loser3, loser4, loser5}

	lt := newLoserTree(losers)
	t.Log(lt.root().value)

	loser1.value = testLoserValue{k: 2, v: 2}
	loser1.contest()
	t.Log(lt.root().value)

	loser1.value = testLoserValue{k: 8, v: 8}
	loser1.contest()
	t.Log(lt.root().value)

	loser2.value = testLoserValue{k: 9, v: 9}
	loser2.contest()
	t.Log(lt.root().value)
}
