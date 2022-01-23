package filesort

import (
	"fmt"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/parser"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestFileSorter_Sharding(t *testing.T) {
	db, _ := database.New("tdsqlshard-n756r9nq.sql.tencentcdb.com", 113, "nico", "Niconico2021@")
	tables, err := parser.ParseTables(db, "D:\\workspace-tencent\\data1")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := New(tables[0])
	if err != nil {
		t.Fatal(err)
	}
	s := time.Now().UnixNano()
	err = fs.Sharding()
	fmt.Println("sharding", (time.Now().UnixNano()-s)/1e6)
	if err != nil {
		t.Fatal(err)
	}

}

type SortSlices []SortSlice

type SortSlice struct {
	id int
}

func (rs SortSlices) Len() int {
	return len(rs)
}

func (rs SortSlices) Less(i, j int) bool {
	return rs[i].id < rs[j].id
}

func (rs SortSlices) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func TestSort(t *testing.T) {
	rows := model.Rows{}
	id := SortSlices{}
	for i := 0; i < 40000*6; i++ {
		k := rand.Intn(1000000000)
		rows = append(rows, model.Row{
			SortID: k,
		})
		id = append(id, SortSlice{
			id: k,
		})
	}
	s := time.Now().UnixNano()
	sort.Sort(rows)
	fmt.Println((time.Now().UnixNano() - s) / 1e6)

	s = time.Now().UnixNano()
	sort.Sort(id)
	fmt.Println((time.Now().UnixNano() - s) / 1e6)
}
