package filesort

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
)

type FileSorter struct {
	sync.Mutex
	sources []*fileBuffer
	shards  []*fileBuffer
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
	ov.l.contest()
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
	paths := strings.Split(path, ",")
	shards := make([]*fileBuffer, 0)
	for _, p := range paths {
		result, err := file.New(p, os.O_RDWR)
		if err != nil {
			return nil, err
		}
		shards = append(shards, newFileBuffer(result, table.Meta))
	}
	return &FileSorter{
		shards: shards,
		table:  table,
	}, nil
}

func (fs *FileSorter) Table() *model.Table {
	return fs.table
}

func (fs *FileSorter) Shards() []*fileBuffer {
	return fs.shards
}

func (fs *FileSorter) newShard() (*fileBuffer, error) {
	fs.Lock()
	defer fs.Unlock()
	f, err := file.New(fmt.Sprintf("%d.shard%d", fs.table.ID, len(fs.shards)), os.O_CREATE|os.O_RDWR|os.O_TRUNC)
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
	path := make([]string, 0)
	for _, shard := range fs.shards {
		path = append(path, shard.f.Path())
	}
	return fs.table.Recover.Make(1, strings.Join(path, ","))
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
			shard, err := fs.newShard()
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

func (fs *FileSorter) Merging(handle func(row *model.Row) error) error {
	losers := make([]*loser, 0)
	for _, shard := range fs.shards {
		_, _ = shard.f.Seek(0, io.SeekStart)
		shard.buf.reset()
		shard.pos = 0
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
	lt := newLoserTree(losers)
	for !lt.root().invalid {
		l := lt.root()
		v := l.value.(*shardLoserValue)
		err := handle(v.row)
		if err != nil {
			return err
		}
		err = v.next()
		if err != nil {
			l.exit()
			continue
		}
		l.contest()
	}
	return nil
}

func (fs *FileSorter) Close() {
	if len(fs.sources) > 0 {
		for _, s := range fs.sources {
			_ = s.f.Close()
		}
	}
	if len(fs.shards) > 0 {
		for _, s := range fs.shards {
			_ = s.f.Close()
		}
	}
}
