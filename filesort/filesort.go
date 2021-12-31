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
	"strings"
	"sync"
)

type FileSorter struct {
	sync.Mutex
	sources []*fileBuffer
	results map[string]*fileBuffer
	table   *model.Table
}

type shardLoserValue struct {
	shard *memBuffer
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
	results := map[string]*fileBuffer{}
	for _, path := range paths {
		infos := strings.Split(path, ":")
		f, err := file.New(infos[1], os.O_RDWR)
		if err != nil {
			return nil, err
		}
		results[infos[0]] = newFileBuffer(f, table.Meta)
	}
	return &FileSorter{
		results: results,
		table:   table,
	}, nil
}

func (fs *FileSorter) Table() *model.Table {
	return fs.table
}

func (fs *FileSorter) Results() map[string]*fileBuffer {
	return fs.results
}

func (fs *FileSorter) newResult(set string) (*fileBuffer, error) {
	f, err := file.New(fmt.Sprintf("%d.result.%s", fs.table.ID, set), os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	return newFileBuffer(f, fs.table.Meta), nil
}

func (fs *FileSorter) Sorting() error {
	wg := sync.WaitGroup{}
	wg.Add(len(fs.sources))
	shards := make([]*memBuffer, 0)
	for i := 0; i < len(fs.sources); i++ {
		source := fs.sources[i]
		go func() {
			defer wg.Add(-1)
			ss, err := fs.shardingSource(source)
			if err != nil {
				log.Error(err)
			}
			fs.Lock()
			defer fs.Unlock()
			shards = append(shards, ss...)
		}()
	}
	wg.Wait()
	return fs.Merging(shards)
}

func (fs *FileSorter) shardingSource(source *fileBuffer) ([]*memBuffer, error) {
	var lastPos int64
	buf := bytes.Buffer{}
	rows := make(model.Rows, 0)
	shards := make([]*memBuffer, 0)
	for {
		row, nextErr := source.NextRow()
		if row != nil {
			rows = append(rows, row)
		}
		if source.pos-lastPos > consts.FileSortShardSize || nextErr != nil {
			lastPos = source.pos
			sort.Sort(&rows)
			shard := newMemBuffer()
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
			shard.rows = rows
			shards = append(shards, shard)
			rows = make(model.Rows, 0)
			buf.Reset()
			if nextErr != nil {
				break
			}
		}
	}
	return shards, nil
}

func (fs *FileSorter) Merging(shards []*memBuffer) error {
	results, err := fs.merging(shards)
	if err != nil {
		log.Error(err)
		return err
	}
	fs.results = results
	infos := bytes.Buffer{}
	for set, result := range results {
		_, _ = result.f.Seek(0, io.SeekStart)
		infos.WriteString(fmt.Sprintf("%s:%s,", set, result.f.Path()))
	}
	infos.Truncate(infos.Len() - 1)
	return fs.table.Recover.Make(1, infos.String())
}

func (fs *FileSorter) merging(shards []*memBuffer) (map[string]*fileBuffer, error) {
	losers := make([]*loser, 0)
	for _, shard := range shards {
		shard.Reset()
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
	results := map[string]*fileBuffer{}
	buffers := map[string]*bytes.Buffer{}
	for _, set := range fs.table.DB.Sets() {
		buffers[set] = &bytes.Buffer{}
		results[set], _ = fs.newResult(set)
	}
	for !lt.root().invalid {
		l := lt.root()
		v := l.value.(*shardLoserValue)
		row := v.row

		set := fs.table.DB.Hash()[util.MurmurHash2([]byte(row.Values[0].Source), 2773)%64]
		buf := buffers[set]
		buf.Write(row.Buffer.Bytes())
		if buf.Len() > consts.FileMergeBufferSize {
			_, err := results[set].f.Write(buf.Bytes())
			if err != nil {
				return nil, err
			}
			buf.Reset()
		}
		err := v.next()
		if err != nil {
			l.exit()
			continue
		}
		l.contest()
	}
	for set, buf := range buffers {
		if buf.Len() > 0 {
			_, err := results[set].f.Write(buf.Bytes())
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func (fs *FileSorter) Close() {
	if len(fs.sources) > 0 {
		for _, s := range fs.sources {
			_ = s.f.Close()
		}
	}
	if len(fs.results) > 0 {
		for _, s := range fs.results {
			_ = s.f.Close()
		}
	}
}
