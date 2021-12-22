package filesort

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"os"
	"sort"
	"sync"
)

type FileSorter struct {
	sync.Mutex
	sources []*fileBuffer
	shards  []*fileBuffer
	table   *model.Table
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

func (fs *FileSorter) newShard(path string) (*fileBuffer, error) {
	fs.Lock()
	defer fs.Unlock()
	f, err := file.New(path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	shard := newFileBuffer(f, fs.table.Meta)
	fs.shards = append(fs.shards, shard)
	return shard, nil
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
		row, nextErr := source.nextRow()
		if row != nil {
			rows = append(rows, row)
		}
		if source.pos-lastPos > consts.FileSortShardSize || nextErr != nil {
			lastPos = source.pos
			sort.Sort(&rows)
			shard, err := fs.newShard(fmt.Sprintf("%d.shard.%d", fs.table.ID, len(fs.shards)))
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
