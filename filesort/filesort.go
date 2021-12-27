package filesort

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/util"
	"io"
	"os"
	"sort"
	"sync"
)

type FileSorter struct {
	sync.Mutex
	sources []*fileBuffer
	shards  []*fileBuffer
	result  *fileBuffer
	table   *model.Table
}

type shardLoserValue struct {
	shard *fileBuffer
	l     *loser
	row   *model.Row
}

func (sv *shardLoserValue) next() error {
	row, err := sv.shard.NextRow()
	if err != nil {
		return err
	}
	sv.row = row
	return nil
}

func (sv *shardLoserValue) Compare(o interface{}) bool {
	ov := o.(*shardLoserValue)
	cur := sv.row
	next := ov.row
	if cur.Key != next.Key {
		return cur.Compare(*next)
	}
	if next.UpdateAt > cur.UpdateAt {
		sv.row = next
	}
	err := ov.next()
	if err != nil {
		ov.row = nil
		ov.l.exit()
		return false
	}
	ov.l.reelect()
	return sv.Compare(ov)
}

func New(table *model.Table) (*FileSorter, error) {
	sources := make([]*fileBuffer, len(table.Sources))
	for i, s := range table.Sources {
		sources[i] = newFileBuffer(s.File, table.Meta)
	}
	return &FileSorter{
		sources: sources,
		table:   table,
	}, nil
}

func Recover(table *model.Table, path string) (*FileSorter, error) {
	return recoverFileSort(table, path)
}

func recoverFileSort(table *model.Table, path string) (*FileSorter, error) {
	result, err := file.New(path, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return &FileSorter{
		result: newFileBuffer(result, table.Meta),
		table:  table,
	}, nil
}

func (fs *FileSorter) Table() *model.Table {
	return fs.table
}

func (fs *FileSorter) Result() *fileBuffer {
	return fs.result
}

func (fs *FileSorter) newShard(tier int) (*fileBuffer, error) {
	fs.Lock()
	defer fs.Unlock()
	f, err := file.New(fmt.Sprintf("%d.shard.%d.%d", fs.table.ID, tier, len(fs.shards)), os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	shard := newFileBuffer(f, fs.table.Meta)
	fs.shards = append(fs.shards, shard)
	return shard, nil
}

func (fs *FileSorter) appendShard(shard *fileBuffer) {
	fs.Lock()
	defer fs.Unlock()
	fs.shards = append(fs.shards, shard)
}

func (fs *FileSorter) Sharding() error {
	fs.shards = make([]*fileBuffer, 0)
	wg := sync.WaitGroup{}
	wg.Add(len(fs.sources))
	for i := 0; i < len(fs.sources); i++ {
		source := fs.sources[i]
		go func() {
			defer wg.Add(-1)
			err := fs.shardingSource(source)
			if err != nil {
				log.Error(err)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (fs *FileSorter) shardingSource(source *fileBuffer) error {
	var lastPos int64
	buf := bytes.Buffer{}
	rows := make(model.Rows, 0)
	for {
		row, nextErr := source.NextRow()
		if row != nil {
			rows = append(rows, row)
		}
		if source.pos-lastPos > consts.FileSortShardSize || nextErr != nil {
			lastPos = source.pos
			sort.Sort(&rows)
			shard, err := fs.newShard(0)
			if err != nil {
				return err
			}
			l := rows.Len()
			for i := 0; i < l; i++ {
				cur := rows[i]
				for j := i + 1; j < l; j++ {
					next := rows[j]
					if cur.Key != next.Key {
						i = j - 1
						break
					}
					i = j
					if next.UpdateAt > cur.UpdateAt {
						cur = next
					}
				}
				buf.Write(cur.Buffer.Bytes())
			}
			_, err = shard.f.Write(buf.Bytes())
			if err != nil {
				return err
			}
			rows = make(model.Rows, 0)
			buf.Reset()
			if nextErr != nil {
				break
			}
		}
	}
	return nil
}

func (fs *FileSorter) Merging() error {
	tier := 1
	multi := len(fs.shards)
	for len(fs.shards) > 1 {
		shards := fs.shards
		fs.shards = make([]*fileBuffer, 0)
		shardsGroup := make([][]*fileBuffer, 0)
		for i := 0; ; i += multi {
			shardsGroup = append(shardsGroup, shards[i:util.Min(len(shards), i+multi)])
			if i+multi >= len(shards) {
				break
			}
		}
		wg := sync.WaitGroup{}
		wg.Add(len(shardsGroup))
		for i := range shardsGroup {
			s := shardsGroup[i]
			go func() {
				defer wg.Add(-1)
				err := fs.merging(s, tier)
				if err != nil {
					log.Error(err)
				}
			}()
		}
		wg.Wait()
		tier++
	}
	fs.result = fs.shards[0]
	_, _ = fs.result.f.Seek(0, io.SeekStart)
	return fs.table.Recover.Make(1, fs.result.f.Path())
}

func (fs *FileSorter) merging(shards []*fileBuffer, tier int) error {
	if len(shards) == 1 {
		fs.appendShard(shards[0])
		return nil
	}
	losers := make([]*loser, 0)
	for _, shard := range shards {
		_, _ = shard.f.Seek(0, io.SeekStart)
		l := &loser{}
		sv := &shardLoserValue{
			shard: shard,
			l:     l,
		}
		l.value = sv
		err := sv.next()
		if err != nil {
			continue
		}
		losers = append(losers, l)
	}
	shard, err := fs.newShard(tier)
	if err != nil {
		return err
	}
	lt := newLoserTree(losers)
	buf := bytes.Buffer{}
	for !lt.root.loser.invalid {
		l := lt.root.loser
		v := l.value.(*shardLoserValue)
		buf.Write(v.row.Buffer.Bytes())
		if buf.Len() > consts.FileMergeBufferSize {
			_, err = shard.f.Write(buf.Bytes())
			if err != nil {
				return err
			}
			buf.Reset()
		}
		err = v.next()
		if err != nil {
			l.exit()
			continue
		}
		l.reelect()
	}
	if buf.Len() > 0 {
		_, err = shard.f.Write(buf.Bytes())
		if err != nil {
			return err
		}
	}
	//for _, s := range shards {
	//	s.Delete()
	//}
	return nil
}
