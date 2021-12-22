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
	dataPath = flag.String("data_path", "D:\\workspace-tencent\\datatest", "dir path of source data")
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
	fss := make([]*filesort.FileSorter, 0)
	for i := range tables {
		fs, err := filesort.New(tables[i])
		if err != nil {
			panic(err)
		}
		if fs.Table().Recover.RowIndex != -1 {
			fss = append(fss, fs)
		}
	}

	fsChan := make(chan *filesort.FileSorter, len(fss))
	sortLimit := make(chan bool, 2)
	syncLimit := make(chan bool, 4)
	for i := 0; i < cap(sortLimit); i++ {
		sortLimit <- true
	}
	for i := 0; i < cap(syncLimit); i++ {
		syncLimit <- true
	}
	go func() {
		for i := range fss {
			_ = <-sortLimit
			fs := fss[i]
			go func() {
				defer func() {
					sortLimit <- true
				}()
				log.Infof("table %d file sort starting\n", fs.Table().ID)
				if fs.Table().Recover.RowIndex == -1 {
					log.Infof("table %d scheduled, skipped\n", fs.Table().ID)
					return
				}
				err := fs.Sharding()
				if err != nil {
					log.Panic(err)
				}
				log.Infof("table %d file sort sharding finished\n", fs.Table().ID)
				err = fs.Merging()
				if err != nil {
					log.Panic(err)
				}
				log.Infof("table %d file sort merging finished\n", fs.Table().ID)
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
	fb := fs.Result()
	buf := bytes.Buffer{}
	offset, err := count(t)
	if err != nil {
		return err
	}
	log.Infof("table %d jumping to %d\n", fs.Table().ID, offset)
	err = fb.Jump(offset)
	if err != nil {
		return err
	}
	log.Infof("table %d start schedule, start from %d\n", fs.Table().ID, offset)
	eof := false
	inserted := 0
	for !eof {
		buf.WriteString(fmt.Sprintf("INSERT INTO %s.%s VALUES ", t.Database, t.Name))
		for i := 0; i < consts.InsertBatch; i++ {
			row, err := fb.NextRow()
			if err != nil {
				eof = true
				break
			}
			buf.WriteString(fmt.Sprintf("(%s),", row.String()))
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString(";")
		_, err = t.DB.Exec(buf.String())
		if err != nil {
			log.Infof("%s.%s err sql: %s\n", t.Database, t.Name, buf.String())
			return err
		}
		inserted += consts.InsertBatch
		if inserted%100*consts.InsertBatch == 0 {
			log.Infof("table %d inserted %d\n", t.ID, inserted)
		}
		buf.Reset()
	}
	fb.Delete()
	err = t.Recover.Make(-1)
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
	sql := strings.ReplaceAll(string(t.Schema), "not exists ", fmt.Sprintf("not exists %s.", t.Database))
	log.Infof("%s.%s create table sql %s\n", t.Database, t.Name, sql)
	_, err = t.DB.Exec(sql)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func count(t *model.Table) (int, error) {
	rows, err := t.DB.Query(fmt.Sprintf("SELECT count(0) FROM %s.%s", t.Database, t.Name))
	if err != nil {
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
