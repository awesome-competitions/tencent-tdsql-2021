package filesort

type loserTree struct {
	losers []*loser
}

type loserValue interface {
	Compare(o interface{}) bool
}

type loser struct {
	loser   *loser
	index   int
	value   loserValue
	lt      *loserTree
	invalid bool
}

func (l *loser) contest() {
	p := (l.index - 1) / 2
	t := l
	lt := l.lt
	i := t.index
	for i > 0 {
		b := i + 1
		if i%2 == 0 {
			b = i - 1
		}
		if t.compare(lt.losers[b]) {
			lt.losers[p] = lt.losers[b]
		} else {
			lt.losers[p] = t
		}
		t = lt.losers[p]
		i = p
		p = (i - 1) / 2
	}
}

func (l *loser) exit() {
	l.invalid = true
	l.contest()
}

func (lt *loserTree) root() *loser {
	return lt.losers[0]
}

func (l *loser) compare(o *loser) bool {
	if l.invalid {
		return true
	}
	if o.invalid {
		return false
	}
	if l.value == nil {
		return true
	}
	if o.value == nil {
		return false
	}
	return l.value.Compare(o.value)
}

func newLoserTree(losers []*loser) *loserTree {
	if len(losers) == 0 {
		return &loserTree{}
	}
	branch := make([]*loser, 0)
	for i := 0; i < len(losers)-1; i++ {
		branch = append(branch, &loser{
			index: i,
		})
	}
	for i := 0; i < len(losers); i++ {
		losers[i].index = len(branch)
		branch = append(branch, losers[i])
	}
	lt := &loserTree{
		losers: branch,
	}
	for i := 0; i < len(branch); i++ {
		branch[i].lt = lt
	}
	for i := len(losers) - 1; i < len(branch); i++ {
		branch[i].contest()
		i++
	}
	return lt
}
