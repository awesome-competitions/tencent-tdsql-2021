package main

import (
	"flag"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/table"
	"sync"
	"sync/atomic"
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
	dataPath = flag.String("data_path", "D:\\workspace\\tencent\\data1", "dir path of source data")
	dstIP = flag.String("dst_ip", "tdsqlshard-n756r9nq.sql.tencentcdb.com", "ip of dst database address")
	dstPort = flag.Int("dst_port", 113, "port of dst database address")
	dstUser = flag.String("dst_user", "nico", "user name of dst database")
	dstPassword = flag.String("dst_password", "Niconico2021@", "password of dst database")

	flag.Parse()
}

func main() {
	start := time.Now().UnixNano()
	_main()
	log.Infof("time-consuming %dms", (time.Now().UnixNano()-start)/1e6)
}

func _main() {
	db, err := database.New(*dstIP, *dstPort, *dstUser, *dstPassword)
	if err != nil {
		log.Panic(err)
	}
	tables, err := table.ParseTables(*dataPath)
	if err != nil {
		log.Panic(err)
	}

	pool := make(chan bool, 4)
	for i := 0; i < cap(pool); i++ {
		pool <- true
	}

	var index int64 = -1
	wg := sync.WaitGroup{}
	wg.Add(len(tables))
	for {
		i := int(atomic.AddInt64(&index, 1))
		if i >= len(tables) {
			break
		}
		select {
		case _ = <-pool:
			go func() {
				defer func() {
					pool <- true
					wg.Add(-1)
				}()
				if err = tables[i].Sync(db); err != nil {
					log.Error(err)
				}
			}()
		}
	}
	wg.Wait()
}
