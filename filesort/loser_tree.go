package filesort

import (
	"github.com/ainilili/tdsql-competition/util"
)

type loserTree struct {
	root *loser
}

type loserValue interface {
	Compare(o interface{}) bool
}

type loser struct {
	loser   *loser
	parent  *loser
	left    *loser
	right   *loser
	value   loserValue
	invalid bool
}

func (l *loser) campaign() {
	if !l.left.compare(l.right) {
		l.loser = l.left.entity()
	} else {
		l.loser = l.right.entity()
	}
}

func (l *loser) reelect() {
	tmp := l.parent
	for tmp != nil {
		tmp.campaign()
		tmp = tmp.parent
	}
}

func (l *loser) exit() {
	l.invalid = true
	l.reelect()
}

func (l *loser) compare(o *loser) bool {
	if o.entity().invalid {
		return false
	}
	if l.entity().invalid {
		return true
	}
	return l.entity().value.Compare(o.entity().value)
}

func (l *loser) entity() *loser {
	if l.loser != nil {
		return l.loser
	}
	return l
}

func (l *loser) setRight(right *loser) {
	l.right = right
	right.parent = l
}

func (l *loser) setLeft(left *loser) {
	l.left = left
	left.parent = l
}

func newLoserTree(losers []*loser) *loserTree {
	if len(losers) == 0 {
		return &loserTree{}
	}
	for {
		if len(losers) == 1 {
			break
		}
		// tmp array to storage tmp parent losers. (fifo queue is better impl)
		tmp := make([]*loser, 0)
		for i := 0; ; i += 2 {
			child := losers[i:util.Min(len(losers), i+2)]
			if len(child) == 1 {
				tmp = append(tmp, child[0])
			} else {
				parent := &loser{}
				parent.setLeft(child[0])
				parent.setRight(child[1])
				child[0].reelect()
				tmp = append(tmp, parent)
			}
			if i+2 >= len(losers) {
				break
			}
		}
		losers = tmp
	}
	if losers[0].loser == nil {
		losers[0].loser = losers[0]
	}
	return &loserTree{
		root: losers[0],
	}
}
