package main

import (
	"flag"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/db"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/parser"
	"strings"
	"sync"
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
	dstIP = flag.String("dst_ip", "", "ip of dst database address")
	dstPort = flag.Int("dst_port", 0, "port of dst database address")
	dstUser = flag.String("dst_user", "", "user name of dst database")
	dstPassword = flag.String("dst_password", "", "password of dst database")
	flag.Parse()
}

func main(){
	tables, err := parser.ParseTables(*dataPath)
	if err != nil{
		log.Panic(err)
	}
	pool, err := db.New(*dstIP, *dstPort, *dstUser, *dstPassword)
	if err != nil{
		log.Panic(err)
	}
	initTableMeta(tables, pool)
}

func initTableMeta(tables []*model.Table, db *db.DB) {
	wg := sync.WaitGroup{}
	wg.Add(len(tables))
	for _, table := range tables {
		go func(table *model.Table) {
			schema, err := table.Schema.ReadAll()
			if err != nil{
				log.Error(err)
				return
			}
			_, err = db.Exec(fmt.Sprintf(consts.CreateDatabaseSqlTemplate, table.Database))
			if err != nil{
				log.Error(err)
				return
			}
			_, err = db.Exec(strings.ReplaceAll(string(schema), "not exists ", fmt.Sprintf("not exists %s.", table.Database)))
			if err != nil{
				log.Error(err)
				return
			}
			meta, err := parser.ParseTableMeta(string(schema))
			if err != nil{
				log.Error(err)
				return
			}
			table.Meta = *meta
			wg.Add(-1)
		}(table)
	}
	wg.Wait()
}

