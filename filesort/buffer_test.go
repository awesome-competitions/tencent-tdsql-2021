package filesort

import (
	"fmt"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/parser"
	"os"
	"testing"
	"time"
)

func TestFileBuffer(t *testing.T) {
	f, err := file.New("D:\\workspace-tencent\\data\\src_a\\a\\1.csv", os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	sql := "CREATE TABLE if not exists `2` (\n  `id` bigint(20) unsigned NOT NULL,\n  `a` float NOT NULL DEFAULT '0',\n  `b` char(32) NOT NULL DEFAULT '',\n  `updated_at` datetime NOT NULL DEFAULT '2021-12-12 00:00:00',\n  PRIMARY KEY (`id`,`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	meta := parser.ParseTableMeta(sql)
	fb := newFileBuffer(f, meta)

	start := time.Now().UnixNano()
	var row *model.Row
	for i := 0; i < 3500000; i++ {
		r, err := fb.NextRow()
		if r != nil {
			row = r
		}
		if err != nil {
			log.Error(err)
			break
		}
	}
	fmt.Println(row.String())
	fmt.Println(fb.readTimes)
	fmt.Println((time.Now().UnixNano()-start)/1e6, "ms")
}
