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
	dataPath = flag.String("data_path", "D:\\workspace\\tencent\\data", "dir path of source data")
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
	tables = tables[:1]

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
				if len(fs.Results()) == 0 {
					log.Infof("table %s file sort starting\n", fs.Table())
					err := fs.Sharding()
					if err != nil {
						log.Panic(err)
					}
					err = fs.Merging()
					if err != nil {
						log.Panic(err)
					}
					log.Infof("table %s file sort merging finished\n", fs.Table())
				}
				fsChan <- fs
			}()
		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(len(fss) * len(db.Sets()))
	go func() {
		for {
			fs := <-fsChan
			for key := range fs.Results() {
				_ = <-syncLimit
				set := key
				go func() {
					defer func() {
						syncLimit <- true
						wg.Add(-1)
					}()
					err := schedule(fs, fs.Table(), set)
					if err != nil {
						log.Panic(err)
					}
				}()
			}
		}
	}()
	wg.Wait()
}

func schedule(fs *filesort.FileSorter, t *model.Table, set string) error {
	err := initTable(t)
	if err != nil {
		log.Error(err)
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
			time.Sleep(500 * time.Millisecond)
			return schedule(fs, t, set)
		}
		return err
	}
	total, err := count(t, set)
	if err != nil {
		log.Error(err)
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
			time.Sleep(500 * time.Millisecond)
			return schedule(fs, t, set)
		}
		return err
	}
	log.Infof("table %s.%s start schedule, start from %v\n", t, set, total)
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("/*sets:%s*/ INSERT INTO %s.%s(%s) VALUES ", set, t.Database, t.Name, t.Cols))
	headerLen := buf.Len()
	fb := fs.Results()[set]
	err = fb.Jump(total)
	if err != nil {
		log.Error(err)
		return err
	}
	prepared := make(chan string, consts.PreparedBatch)
	completed := false
	eof := false
	go func() {
		for !eof {
			for i := 0; i < consts.InsertBatch; i++ {
				row, err := fb.NextRow()
				if err != nil {
					eof = true
					break
				}
				buf.WriteString(fmt.Sprintf("(%s),", row.String()))
			}
			if buf.Len() > headerLen {
				buf.Truncate(buf.Len() - 1)
				buf.WriteString(";")
				prepared <- buf.String()
				buf.Truncate(headerLen)
			}
		}
		prepared <- ""
	}()

	for !completed {
		select {
		case s := <-prepared:
			if s == "" {
				completed = true
				break
			}
			_, err = t.DB.Exec(s)
			if err != nil {
				log.Errorf("table %s.%s sql err: %v\n", t, set, err)
				if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
					completed = true
					time.Sleep(500 * time.Millisecond)
					return schedule(fs, t, set)
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
	total, _ = count(t, set)
	log.Infof("table %s.%s total %v\n", t, set, total)
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
