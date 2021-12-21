package filesort

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/model"
	"os"
	"sort"
)

type FileSorter struct {
	sources []*fileBuffer
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
	f, err := file.New("D:\\workspace\\tencent\\tmp\\"+path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return newFileBuffer(f, fs.table.Meta), nil
}

func (fs *FileSorter) Sharding() error {
	shards := make([]*fileBuffer, 0)

	var lastPos int64
	rows := make(model.Rows, 0)
	buf := bytes.Buffer{}
	total := 0
	meta := fs.table.Meta
	cols := meta.Keys
	if len(cols) == 0 {
		cols = meta.Cols
	}
	tags := make([]int, 0)
	for _, col := range cols {
		if col != "updated_at" {
			tags = append(tags, meta.ColsIndex[col])
		}
	}
	updatedAt := meta.ColsIndex["updated_at"]
	for _, source := range fs.sources {
		for {
			row, nextErr := source.nextRow()
			if row != nil {
				rows = append(rows, row)
			}
			if source.pos-lastPos > consts.FileSortShardSize || nextErr != nil {
				lastPos = source.pos
				sort.Sort(&rows)
				shard, err := fs.newShard(fmt.Sprintf("%d.shard.%d", fs.table.ID, len(shards)))
				if err != nil {
					return err
				}
				shards = append(shards, shard)
				l := rows.Len()
				for i := 0; i < l; i++ {
					cur := rows[i]
					for j := i + 1; j < l; j++ {
						next := rows[j]
						same := true
						for _, t := range tags {
							if !cur.Values[t].Equals(next.Values[t]) {
								same = false
								break
							}
						}
						if !same {
							i = j - 1
							break
						}
						if next.Values[updatedAt].Compare(cur.Values[updatedAt]) > 0 {
							cur = next
						}
					}
					buf.Write(cur.Buffer.Bytes())
				}
				total += rows.Len()
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
		lastPos = 0
	}
	fmt.Println(total)
	return nil
}
