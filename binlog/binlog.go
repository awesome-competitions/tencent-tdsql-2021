package binlog

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/util"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"os"
	"strings"
	"time"
)

func Listen(ip string, port uint16, usr, pwd string, gtid string) {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     ip,
		Port:     port,
		User:     usr,
		Password: pwd,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	startGtid, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtid)
	if err != nil {
		panic(err)
	}
	streamer, _ := syncer.StartSyncGTID(startGtid)
	handleEvent(streamer)
}

func handleEvent(streamer *replication.BinlogStreamer) {
	files := map[string]*file.File{}
	buffer := bytes.Buffer{}
	eventTypes := map[replication.EventType]bool{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()
		if err == context.DeadlineExceeded {
			break
		}
		eventTypes[ev.Header.EventType] = true
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			flag := string(rowsEvent.Table.Schema) + string(rowsEvent.Table.Table)
			if _, ok := files[flag]; !ok {
				f, err := file.New(flag+".csv", 0766)
				if err != nil {
					panic(err)
				}
				files[flag] = f
			}
			f := files[flag]
			if f == nil {
				rowsEvent.Table.Dump(os.Stdout)
				rowsEvent.Dump(os.Stdout)
				panic("f is nil")
			}
			for _, row := range rowsEvent.Rows {
				for _, columnData := range row {
					v := util.String(columnData)
					buffer.WriteString(v + ",")
				}
				buffer.Truncate(buffer.Len() - 1)
				buffer.WriteString("\n")
			}
			if buffer.Len() > 0 {
				_, err := f.Write(buffer.Bytes())
				if err != nil {
					panic(err)
				}
				buffer.Reset()
			}
		} else if queryEvent, ok := ev.Event.(*replication.QueryEvent); ok {
			query := string(queryEvent.Query)
			if strings.HasPrefix(query, "DROP") || strings.HasPrefix(query, "ALTER") {
				fmt.Println(query)
			}
			//if strings.HasPrefix(query, "DROP") {
			//	for k := range files {
			//		if strings.HasPrefix(k, string(queryEvent.Schema)) {
			//			_ = files[k].Delete()
			//			delete(files, k)
			//			fmt.Println("delete", k)
			//		}
			//	}
			//}
		}
	}
	for k := range eventTypes {
		fmt.Println(k)
	}
	for _, v := range files {
		fmt.Println(v.Name(), v.Size())
	}
}
