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
	shards  map[string][]*fileBuffer
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
	shards := map[string][]*fileBuffer{}
	setInfos := strings.Split(path, ";")
	for _, setInfo := range setInfos {
		infos := strings.Split(setInfo, ":")
		set := infos[0]
		files := strings.Split(infos[1], ",")
		s := make([]*fileBuffer, 0)
		for _, fp := range files {
			f, err := file.New(fp, os.O_RDWR)
			if err != nil {
				return nil, err
			}
			s = append(s, newFileBuffer(f, table.Meta))
		}
		shards[set] = s
	}
	fs := &FileSorter{
		shards: shards,
		table:  table,
	}
	return fs, nil
}

func (fs *FileSorter) InitLts(set string) *loserTree {
	losers := make([]*loser, 0)
	for _, shard := range fs.shards[set] {
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
	return newLoserTree(losers)
}

func (fs *FileSorter) Table() *model.Table {
	return fs.table
}

func (fs *FileSorter) Shards() map[string][]*fileBuffer {
	return fs.shards
}

func (fs *FileSorter) newShard(set string) (*fileBuffer, error) {
	fs.Lock()
	defer fs.Unlock()
	f, err := file.New(fmt.Sprintf("%d_shard_%s_%d", fs.table.ID, set, len(fs.shards[set])), os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	shard := newFileBuffer(f, fs.table.Meta)
	fs.shards[set] = append(fs.shards[set], shard)
	return shard, nil
}

func (fs *FileSorter) appendShard(set string, shard *fileBuffer) {
	fs.Lock()
	defer fs.Unlock()
	fs.shards[set] = append(fs.shards[set], shard)
}

func (fs *FileSorter) Sharding() error {
	shards := map[string][]*fileBuffer{}
	fs.shards = shards
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
	path := bytes.Buffer{}
	for set, shards := range fs.shards {
		path.WriteString(set + ":")
		for _, s := range shards {
			path.WriteString(s.f.Path() + ",")
		}
		path.Truncate(path.Len() - 1)
		path.WriteString(";")
	}
	path.Truncate(path.Len() - 1)
	return fs.table.Recover.Make(1, path.String())
}

func (fs *FileSorter) shardingSource(source *fileBuffer) error {
	var lastPos int64
	buf := bytes.Buffer{}
	rows := map[string]model.Rows{}
	for {
		row, nextErr := source.NextRow()
		if row != nil {
			set := fs.table.DB.Hash()[util.MurmurHash2([]byte(row.Values[0].Source), 2773)%64]
			rows[set] = append(rows[set], row)
		}
		if source.pos-lastPos > consts.FileSortShardSize || nextErr != nil {
			lastPos = source.pos
			for set, rs := range rows {
				sort.Sort(&rs)
				shard, err := fs.newShard(set)
				if err != nil {
					return err
				}
				l := rs.Len()
				for i := 0; i < l; i++ {
					cur := rs[i]
					for j := i + 1; j < l; j++ {
						next := rs[j]
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
				buf.Reset()
			}
			rows = map[string]model.Rows{}
			if nextErr != nil {
				break
			}
		}
	}
	return nil
}

func (fs *FileSorter) Next(lt *loserTree, set string) (*model.Row, error) {
	if lt.root().invalid {
		return nil, io.EOF
	}
	l := lt.root()
	v := l.value.(*shardLoserValue)
	row := v.row
	err := v.next()
	if err != nil {
		l.exit()
	} else {
		l.contest()
	}
	return row, nil
}

func (fs *FileSorter) HasNext(lt *loserTree, set string) bool {
	return !lt.root().invalid
}

func (fs *FileSorter) LastPositions(set string) []int64 {
	shards := fs.shards[set]
	positions := make([]int64, len(shards))
	for i, s := range shards {
		positions[i] = s.LastPosition()
	}
	return positions
}

func (fs *FileSorter) ResetPositions(set string, positions []int64) {
	shards := fs.shards[set]
	for i, s := range shards {
		s.Reset(positions[i])
	}
}

func (fs *FileSorter) Close() {
	if len(fs.sources) > 0 {
		for _, s := range fs.sources {
			_ = s.f.Close()
		}
	}
}
