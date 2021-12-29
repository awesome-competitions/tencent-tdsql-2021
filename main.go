package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/filesort"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/parser"
	"github.com/ainilili/tdsql-competition/util"
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
	dataPath = flag.String("data_path", "D:\\workspace-tencent\\data1", "dir path of source data")
	dstIP = flag.String("dst_ip", "tdsqlshard-n756r9nq.sql.tencentcdb.com", "ip of dst database address")
	dstPort = flag.Int("dst_port", 113, "port of dst database address")
	dstUser = flag.String("dst_user", "nico", "user name of dst database")
	dstPassword = flag.String("dst_password", "Niconico2021@", "password of dst database")
	flag.Parse()
}

func main() {
	start := time.Now().UnixNano()
	_main()
	log.Infof("time-consuming %dms\n", (time.Now().UnixNano()-start)/1e6)
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

	fsChan := make(chan *filesort.FileSorter, len(tables))
	sortLimit := make(chan bool, consts.FileSortLimit)
	syncLimit := make(chan bool, consts.SyncLimit)
	for i := 0; i < cap(sortLimit); i++ {
		sortLimit <- true
	}
	for i := 0; i < cap(syncLimit); i++ {
		syncLimit <- true
	}
	fss := make([]*filesort.FileSorter, 0)
	for i := range tables {
		fg, path, err := tables[i].Recover.Load()
		if err != nil {
			log.Panic(err)
		}
		if fg == 0 {
			fs, err := filesort.New(tables[i])
			if err != nil {
				log.Panic(err)
			}
			fss = append(fss, fs)
		} else if fg == 1 {
			fs, err := filesort.Recover(tables[i], path)
			if err != nil {
				log.Panic(err)
			}
			fss = append(fss, fs)
		}
	}

	go func() {
		for i := range fss {
			_ = <-sortLimit
			fs := fss[i]
			go func() {
				defer func() {
					sortLimit <- true
				}()
				if len(fs.Shards()) == 0 {
					log.Infof("table %s file sort starting\n", fs.Table())
					err := fs.Sharding()
					if err != nil {
						log.Panic(err)
					}
					log.Infof("table %s file sort sharding finished\n", fs.Table())
				}
				fsChan <- fs
			}()
		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(len(fss))
	go func() {
		for {
			fs := <-fsChan
			_ = <-syncLimit
			go func() {
				defer func() {
					syncLimit <- true
					wg.Add(-1)
				}()
				err := schedule(fs)
				if err != nil {
					log.Panic(err)
				}
			}()
		}
	}()
	wg.Wait()
}

func schedule(fs *filesort.FileSorter) error {
	t := fs.Table()
	err := initTable(t)
	if err != nil {
		return err
	}
	offset, err := count(t)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("table %s start schedule, start from %d\n", fs.Table(), offset)

	index := 0
	batch := 4
	buffered := 0
	inserted := 0
	headerLen := 0

	bmap := map[string]*bytes.Buffer{}
	for _, set := range t.DB.Sets() {
		bmap[set] = &bytes.Buffer{}
		bmap[set].WriteString(fmt.Sprintf("/*sets:%s*/ INSERT INTO %s.%s(%s) VALUES ", set, t.Database, t.Name, t.Cols))
		headerLen = bmap[set].Len()
	}
	buffers := make([]*bytes.Buffer, len(t.DB.Hash()))
	for i, set := range t.DB.Hash() {
		buffers[i] = bmap[set]
	}
	prepared := make(chan string, batch)
	go func() {
		// todo merging 区分不同分片，count也随着分片走
		mErr := fs.Merging(func(row *model.Row) error {
			if index < offset {
				index++
				return nil
			}
			buf := buffers[util.MurmurHash2([]byte(row.Values[0].Source), 2773)%64]
			buf.WriteString(fmt.Sprintf("(%s),", row.String()))

			buffered++
			inserted++
			if buffered == consts.InsertBatch {
				buf.Truncate(buf.Len() - 1)
				buf.WriteString(";")
				prepared <- buf.String()
				buffered = 0
				buf.Truncate(headerLen)
			}
			return nil
		})
		if buffered > 0 {
			for _, buf := range bmap {
				if buf.Len() == headerLen {
					continue
				}
				buf.Truncate(buf.Len() - 1)
				buf.WriteString(";")
				prepared <- buf.String()
			}
		}
		prepared <- ""
		if mErr != nil {
			log.Error(mErr)
		}
	}()
	completed := false
	for !completed {
		select {
		case buf := <-prepared:
			if buf == "" {
				completed = true
				break
			}
			_, err = t.DB.Exec(buf)
			if err != nil {
				log.Errorf("table %s sql err: %v\n", t, err)
				if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
					time.Sleep(500 * time.Millisecond)
					return schedule(fs)
				}
				return err
			}
		}
	}

	fs.Close()
	err = t.Recover.Make(2, "")
	if err != nil {
		return err
	}
	total, err := count(t)
	if err != nil {
		return err
	}
	log.Infof("table %s.%s total %d\n", t.Database, t.Name, total)
	return nil
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

func count(t *model.Table) (int, error) {
	rows, err := t.DB.Query(fmt.Sprintf("SELECT count(id) FROM %s.%s as a", t.Database, t.Name))
	if err != nil {
		log.Error(err)
		return 0, err
	}
	total := 0
	if rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
