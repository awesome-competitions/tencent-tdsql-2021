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
	results map[string]*fileBuffer
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
	if next.UpdateAt() > cur.UpdateAt() {
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
		results: map[string]*fileBuffer{},
	}, nil
}

func (fs *FileSorter) Results() map[string]*fileBuffer {
	return fs.results
}

func (fs *FileSorter) GetResults(set string) *fileBuffer {
	fs.Lock()
	defer fs.Unlock()
	return fs.results[set]
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

func (fs *FileSorter) newResult(set string) (*fileBuffer, error) {
	fs.Lock()
	defer fs.Unlock()
	f, err := file.New(fmt.Sprintf("%d_result_%s", fs.table.ID, set), os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	result := newFileBuffer(f, fs.table.Meta)
	fs.results[set] = result
	return result, nil
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
	return nil
}

func (fs *FileSorter) shardingSource(source *fileBuffer) error {
	var lastPos int64
	buf := bytes.Buffer{}
	rows := map[string]model.Rows{}
	for {
		row, nextErr := source.NextRow()
		if row != nil {
			set := fs.table.DB.Hash()[util.MurmurHash2([]byte(row.ID()), 2773)%64]
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
						if next.UpdateAt() > cur.UpdateAt() {
							cur = next
						}
					}
					buf.WriteString(cur.String() + "\n")
				}
				_, err = shard.f.Write(buf.Bytes())
				if err != nil {
					return err
				}
				shard.Reset(consts.FileBufferSize)
				shard.buf.buf = []byte(string(buf.Bytes()[0:consts.FileBufferSize]))
				shard.buf.cap = consts.FileBufferSize
				shard.buf.pos = 0
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

func (fs *FileSorter) Merging() error {
	wg := sync.WaitGroup{}
	wg.Add(len(fs.shards))
	for set := range fs.shards {
		shards := fs.shards[set]
		s := set
		go func() {
			defer wg.Add(-1)
			err := fs.merging(s, shards)
			if err != nil {
				log.Error(err)
				return
			}
			go func() {
				for _, s := range shards {
					_ = s.f.Close()
				}
			}()
		}()
	}
	wg.Wait()
	infos := bytes.Buffer{}
	for set, result := range fs.results {
		_, _ = result.f.Seek(0, io.SeekStart)
		infos.WriteString(fmt.Sprintf("%s:%s,", set, result.f.Path()))
	}
	infos.Truncate(infos.Len() - 1)
	return fs.table.Recover.Make(1, infos.String())
}

func (fs *FileSorter) merging(set string, shards []*fileBuffer) error {
	losers := make([]*loser, 0)
	for _, shard := range shards {
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
	result, err := fs.newResult(set)
	if err != nil {
		return err
	}
	buffer := &bytes.Buffer{}
	for !lt.root().invalid {
		l := lt.root()
		v := l.value.(*shardLoserValue)
		row := v.row
		buffer.WriteString(row.Source + "\n")
		if buffer.Len() > consts.FileMergeBufferSize {
			_, err := result.f.Write(buffer.Bytes())
			if err != nil {
				return err
			}
			buffer.Reset()
		}
		err := v.next()
		if err != nil {
			l.exit()
			continue
		}
		l.contest()
	}
	if buffer.Len() > 0 {
		_, err := result.f.Write(buffer.Bytes())
		if err != nil {
			return err
		}
	}
	result.Reset(0)
	return nil
}

func (fs *FileSorter) Next(lt *loserTree, set string) (*model.Row, error) {
	if !fs.HasNext(lt, set) {
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
	return !(len(lt.losers) == 0 || lt.root() == nil || lt.root().invalid)
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
		if positions[i] > 0 {
			s.Reset(positions[i])
		}
	}
}

func (fs *FileSorter) Close() {
	if len(fs.sources) > 0 {
		for _, s := range fs.sources {
			_ = s.f.Close()
		}
	}
}
