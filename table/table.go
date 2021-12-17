package table

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/util"
	"sort"
	"strings"
)

type Table struct {
	Name     string
	Database string
	Data     []Data
	Schema   *file.File
	Meta     Meta
}

type Data struct {
	DataSource string
	Data       *file.File
}

type Meta struct {
	Keys      []string
	Cols      []string
	ColsIndex map[string]int
	ColsType  map[string]Type
}

func (t *Table) Sync(db *database.DB) error {
	t.initMeta(db)
	log.Infof("sync database %s, table %s \n", t.Database, t.Name)
	rows, err := t.loadData()
	if err != nil {
		return err
	}
	for _, row := range rows {
		log.Info(row)
	}
	//return t.insertInto(db, rows)
	return nil
}

func (t *Table) insertInto(db *database.DB, rows Rows) error {
	offset := 0
	buff := bytes.Buffer{}
	for {
		buff.WriteString(fmt.Sprintf("INSERT INTO %s.%s VALUES ", t.Database, t.Name))
		for i := offset; i < util.Min(offset+consts.InsertBatch, rows.Len()); i++ {
			buff.WriteString(fmt.Sprintf("(%s),", rows[i].String()))
		}
		buff.Truncate(buff.Len() - 1)
		buff.WriteString(";")
		result, err := db.Exec(buff.String())
		if err != nil {
			log.Infof("err sql: %s\n", buff.String())
			return err
		}
		affected, _ := result.RowsAffected()
		if affected < 500 {
			break
		}
		offset += consts.InsertBatch
		buff.Reset()
	}
	return nil
}

func (t *Table) loadData() (Rows, error) {
	row := make(Row, 0, 5)
	rows := make(Rows, 0)
	index := map[string]Row{}
	for _, data := range t.Data {
		byteArr, err := data.Data.ReadAll()
		if err != nil {
			return nil, err
		}
		buf := bytes.Buffer{}
		i := 0
		for _, b := range byteArr {
			if b == consts.COMMA || b == consts.LF {
				tp := t.Meta.ColsType[t.Meta.Cols[i]]
				i++
				source := buf.String()
				row = append(row, Value{
					T: tp,
					V: TypeParser[tp](source),
					S: source,
				})
				if b == consts.LF {
					buf.Reset()
					tags := t.Meta.Keys
					if len(tags) == 0 {
						tags = t.Meta.Cols[:len(t.Meta.Cols)-1]
					}
					for _, tag := range tags {
						buf.WriteString(row[t.Meta.ColsIndex[tag]].S + ":")
					}
					exist, ok := index[buf.String()]
					if !ok {
						index[buf.String()] = row
						rows = append(rows, row)
					} else {
						updateAtIndex := t.Meta.ColsIndex[consts.UpdateAtColumnName]
						if exist[updateAtIndex].Compare(row[updateAtIndex]) < 0 {
							copy(exist, row)
						}
					}
					row = make(Row, 0, 5)
					i = 0
				}
				buf.Reset()
				continue
			}
			buf.WriteByte(b)
		}
	}
	sort.Sort(&rows)
	return rows, nil
}

func (t *Table) initMeta(db *database.DB) {
	schema, err := t.Schema.ReadAll()
	if err != nil {
		log.Error(err)
		return
	}
	_, err = db.Exec(fmt.Sprintf(consts.CreateDatabaseSqlTemplate, t.Database))
	if err != nil {
		log.Error(err)
		return
	}
	_, err = db.Exec(strings.ReplaceAll(string(schema), "not exists ", fmt.Sprintf("not exists %s.", t.Database)))
	if err != nil {
		log.Error(err)
		return
	}
	meta, err := parseTableMeta(string(schema))
	if err != nil {
		log.Error(err)
		return
	}
	t.Meta = *meta
}
