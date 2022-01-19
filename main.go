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
	"strconv"
	"strings"
	"sync"
	"time"
)

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string

type Task struct {
	Fs  *filesort.FileSorter
	Set string
}

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

	tasks := make(chan *Task, 100)
	sortLimit := make(chan bool, consts.FileSortLimit)
	syncLimits := map[string]chan bool{}
	limit := consts.SyncLimit
	for _, set := range db.Sets() {
		syncLimits[set] = make(chan bool, limit)
		for i := 0; i < limit; i++ {
			syncLimits[set] <- true
		}
	}
	for i := 0; i < cap(sortLimit); i++ {
		sortLimit <- true
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
				for set := range fs.Results() {
					tasks <- &Task{
						Fs:  fs,
						Set: set,
					}
				}
			}()
		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(len(fss) * len(db.Sets()))
	go func() {
		for {
			task := <-tasks
			go func() {
				set := task.Set
				_ = <-syncLimits[set]
				defer func() {
					syncLimits[set] <- true
					wg.Add(-1)
				}()
				err := schedule(task.Fs, set)
				if err != nil {
					log.Panic(err)
				}
			}()
		}
	}()
	wg.Wait()
}

func schedule(fs *filesort.FileSorter, set string) error {
	t := fs.Table()
	fg, record, _ := t.SetRecovers[set].Load()
	if fg == 1 {
		return nil
	}
	err := initTable(t)
	if err != nil {
		log.Error(err)
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
			time.Sleep(500 * time.Millisecond)
			return schedule(fs, set)
		}
		return err
	}

	buf := bytes.Buffer{}
	recordBuf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("/*sets:%s*/ INSERT INTO %s.%s(%s) VALUES ", set, t.Database, t.Name, t.Cols))
	headerLen := buf.Len()

	log.Infof("table %s_%s start jump\n", t, set)
	c, err := count(t, set)
	if err != nil {
		log.Error(err)
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
			time.Sleep(500 * time.Millisecond)
			return schedule(fs, set)
		}
		return err
	}

	result := fs.GetResults(set)

	total := int64(0)
	pos := int64(0)
	lastTotal := int64(0)
	lastPos := int64(0)
	if len(record) > 0 {
		infos := strings.Split(record, ",")
		total, _ = strconv.ParseInt(infos[0], 10, 64)
		pos, _ = strconv.ParseInt(infos[1], 10, 64)
		lastTotal, _ = strconv.ParseInt(infos[2], 10, 64)
		lastPos, _ = strconv.ParseInt(infos[3], 10, 64)
	}
	if c == int(lastTotal) {
		pos = lastPos
		total = lastTotal
	}
	lastPos = pos
	lastTotal = total

	result.Reset(pos)
	log.Infof("table %s_%s start schedule\n", t, set)
	prepared := make(chan model.Sql, consts.PreparedBatch)
	completed := false
	sqlErr := false
	eof := false
	go func() {
		for !eof && !sqlErr {
			inserted := 0
			for i := 0; i < consts.InsertBatch; i++ {
				row, err := result.NextRow()
				if sqlErr || err != nil {
					eof = true
					break
				}
				buf.WriteString(fmt.Sprintf("(%s),", row.String()))
				inserted++
			}
			if inserted > 0 {
				buf.Truncate(buf.Len() - 1)
				buf.WriteString(";")
				total += int64(inserted)
				pos = result.Position()

				recordBuf.Reset()
				recordBuf.WriteString(fmt.Sprintf("%d,%d,%d,%d", total, pos, lastTotal, lastPos))
				prepared <- model.Sql{
					Sql:      buf.String(),
					Record:   recordBuf.String(),
					Finished: eof,
				}
				lastPos = pos
				lastTotal = total
				buf.Truncate(headerLen)
			}
		}
		if sqlErr {
			prepared <- model.Sql{
				Sql: "sqlErr",
			}
			return
		}
		prepared <- model.Sql{
			Sql: "",
		}
	}()

	ctx := context.Background()
	conn, _ := t.DB.GetConn(ctx)
	for !completed {
		select {
		case s := <-prepared:
			if s.Sql == "sqlErr" {
				time.Sleep(500 * time.Millisecond)
				return schedule(fs, set)
			}
			if s.Sql == "" {
				completed = true
				if !sqlErr {
					_ = t.SetRecovers[set].Make(1, s.Record)
				}
				break
			}
			if !sqlErr {
				_ = t.SetRecovers[set].Make(0, s.Record)
				st := time.Now().UnixNano()
				_, err = conn.ExecContext(ctx, s.Sql)
				log.Infof("table %s_%s exec sql-consuming %dms\n", t, set, (time.Now().UnixNano()-st)/1e6)
				if err != nil {
					fmt.Printf("table %s_%s sql err: %v\n", t, set, err)
					if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
						sqlErr = true
					} else {
						return err
					}
				}
			}
		}
	}
	if sqlErr {
		time.Sleep(500 * time.Millisecond)
		return schedule(fs, set)
	}
	c, _ = count(t, set)
	log.Infof("table %s_%s finished! total %v\n", t, set, c)
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
