package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/filesort"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/parser"
	"github.com/ainilili/tdsql-competition/util"
	"github.com/bits-and-blooms/bloom/v3"
	"io"
	"strings"
	"sync"
	"time"
)

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string

//  example of parameter parse, the final binary should be able to accept specified parameters as requested
//
//  usage example:
//      ./run --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
//
//  you can test this example by:
//  go run main.go --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
func init() {
	dataPath = flag.String("data_path", "D:\\workspace-tencent\\data", "dir path of source data")
	dstIP = flag.String("dst_ip", "tdsqlshard-n756r9nq.sql.tencentcdb.com", "ip of dst database address")
	dstPort = flag.Int("dst_port", 113, "port of dst database address")
	dstUser = flag.String("dst_user", "nico", "user name of dst database")
	dstPassword = flag.String("dst_password", "Niconico2021@", "password of dst database")
	flag.Parse()
}

func main() {
	start := time.Now().UnixNano()
	_main()
	fmt.Printf("time-consuming %dms\n", (time.Now().UnixNano()-start)/1e6)
}

func _main() {
	db, err := database.New(*dstIP, *dstPort, *dstUser, *dstPassword)
	if err != nil {
		log.Panic(err)
	}
	tables, err := parser.ParseTables(db, *dataPath)
	if err != nil {
		log.Panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(tables))
	for i := range tables {
		fg, pos, err := tables[i].Recover.Load()
		if err != nil {
			log.Panic(err)
		}
		t := tables[i]
		go func() {
			defer func() {
				wg.Add(-1)
			}()
			if fg == 0 {
				err := initTable(t)
				if err != nil {
					log.Panic(err)
				}
			}
			if fg == -1 {
				return
			}
			filter := bloom.NewWithEstimates(10000000, 0.01)
			for ; fg < len(t.Sources); fg++ {
				log.Infof("%s sync fg %d\n", t, fg)
				err := schedule(t, filter, fg, pos)
				if err != nil {
					log.Panic(err)
				}
			}
			err = t.Recover.Make(-1, 0)
			if err != nil {
				log.Panic(err)
			}
		}()
	}
	wg.Wait()
}

func schedule(t *model.Table, filter *bloom.BloomFilter, flag int, pos int64) error {
	fileBuffer := filesort.NewFileBuffer(t.Sources[flag].File, t.Meta)
	fileBuffer.Reset(pos)

	buffers := map[string]*model.Buffer{}
	for _, set := range t.DB.Sets() {
		buffers[set] = &model.Buffer{
			Buffer: &bytes.Buffer{},
		}
		buffers[set].Buffer.WriteString(fmt.Sprintf("/*sets:%s*/ INSERT INTO %s.%s(%s) VALUES ", set, t.Database, t.Name, t.Cols))
		buffers[set].HeaderSize = buffers[set].Buffer.Len()
	}
	queries := make(chan model.Query, consts.PreparedBatch)
	finished := false
	go func() {
		for !finished {
			inserted := 0
			for i := 0; i < consts.InsertBatch; i++ {
				row, err := fileBuffer.NextRow()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Panic(err)
				}
				if filter.TestOrAddString(row.Key) {
					continue
				}
				set := t.DB.Hash()[util.MurmurHash2([]byte(row.ID), 2773)%64]
				buffer := buffers[set]
				buffer.Buffer.WriteString(fmt.Sprintf("(%s),", row.String()))
				buffer.BufferSize++
				inserted++
			}
			if inserted > 0 {
				inserted = 0
				sql := make([]string, 0)
				for _, buffer := range buffers {
					if buffer.BufferSize > 0 {
						buffer.Buffer.Truncate(buffer.Buffer.Len() - 1)
						buffer.Buffer.WriteString(";")
						sql = append(sql, buffer.Buffer.String())
					}
					buffer.Buffer.Reset()
				}
				queries <- model.Query{
					Sql: sql,
					Pos: fileBuffer.Position(),
				}
			}
		}
		queries <- model.Query{
			Finished: true,
		}
	}()

	ctx := context.Background()
	conn, err := t.DB.GetConn(ctx)
	if err != nil {
		log.Error(err)
		return err
	}
	wg := sync.WaitGroup{}

	for {
		select {
		case query := <-queries:
			if query.Finished {
				return nil
			}
			wg.Add(len(query.Sql))
			for i := range query.Sql {
				s := query.Sql[i]
				go func() {
					defer wg.Add(-1)
					st := time.Now().UnixNano()
					_, err = conn.ExecContext(ctx, s)
					log.Infof("table %s exec sql-consuming %dms\n", t, (time.Now().UnixNano()-st)/1e6)
					if err != nil {
						log.Panic(err)
					}
				}()
			}
			wg.Wait()
		}
	}
}

func initTable(t *model.Table) error {
	_, err := t.DB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin';", t.Database))
	if err != nil {
		log.Error(err)
		return err
	}
	sql := strings.ReplaceAll(t.Schema, "not exists ", fmt.Sprintf("not exists %s.", t.Database))
	shardKey := ""
	if len(t.Meta.PrimaryKeys) == 0 {
		sql = strings.ReplaceAll(sql, ") ENGINE=InnoDB", fmt.Sprintf(",PRIMARY KEY (%s)\n) ENGINE=InnoDB", t.Cols[:strings.LastIndex(t.Cols, ",")]))
		shardKey = t.Meta.Cols[0]
	} else {
		shardKey = t.Meta.PrimaryKeys[0]
	}
	sql = strings.ReplaceAll(sql, "ENGINE=InnoDB", "ENGINE=InnoDB shardkey="+shardKey)
	_, err = t.DB.Exec(sql)
	if err != nil {
		log.Error(err)
		log.Error(sql)
		return err
	}
	return nil
}

func count(t *model.Table, set string) (int, error) {
	rows, err := t.DB.Query(fmt.Sprintf("/*sets:%s*/ SELECT count(id) FROM %s.%s as a", set, t.Database, t.Name))
	if err != nil {
		log.Error(err)
		return 0, err
	}
	total := 0
	str := ""
	for rows.Next() {
		err = rows.Scan(&total, &str)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
