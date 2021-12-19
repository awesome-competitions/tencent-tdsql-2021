package table

import (
	"bytes"
	"fmt"
	"github.com/ainilili/tdsql-competition/consts"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/rver"
	"github.com/ainilili/tdsql-competition/util"
	"sort"
	"strings"
)

type Table struct {
	ID       int
	Name     string
	Database string
	Data     []Data
	Schema   *file.File
	Meta     Meta
	DB       *database.DB
	Recover  *rver.Recover
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

func (t *Table) Init() (Rows, error) {
	err := t.initRecover()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	err = t.initMeta()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if t.Recover.RowIndex < 0 {
		log.Infof("sync %s.%s already synced, skipped!\n", t.Database, t.Name)
		return nil, nil
	}
	log.Infof("sync %s.%s \n", t.Database, t.Name)
	rows, err := t.loadData()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (t *Table) Sync(rows Rows) error {
	if rows.Len() == 0 {
		return nil
	}
	return nil
}

func (t *Table) insertInto(rows Rows) error {
	buff := bytes.Buffer{}
	offset, err := t.count()
	if err != nil {
		return err
	}
	log.Infof("%s.%s start sync from offset %d\n", t.Database, t.Name, offset)
	for {
		buff.WriteString(fmt.Sprintf("INSERT INTO %s.%s VALUES ", t.Database, t.Name))
		for i := offset; i < util.Min(offset+consts.InsertBatch, rows.Len()); i++ {
			buff.WriteString(fmt.Sprintf("(%s),", rows[i].String()))
		}
		offset += consts.InsertBatch
		buff.Truncate(buff.Len() - 1)
		buff.WriteString(";")
		_, err := t.DB.Exec(buff.String())
		if err != nil {
			log.Infof("%s.%s err sql: %s\n", t.Database, t.Name, buff.String())
			return err
		}
		if rows.Len() <= offset {
			break
		}
		buff.Reset()
	}
	log.Infof("%s.%s sync finished\n", t.Database, t.Name)
	return t.Recover.Make(-1)
}

func (t *Table) count() (int, error) {
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

func (t *Table) initMeta() error {
	schema, err := t.Schema.ReadAll()
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = t.DB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin';", t.Database))
	if err != nil {
		log.Error(err)
		return err
	}
	sql := strings.ReplaceAll(string(schema), "not exists ", fmt.Sprintf("not exists %s.", t.Database))
	sql = strings.ReplaceAll(sql, "float", "float(32,16)")
	sql = strings.ReplaceAll(sql, "double", "double(32,16)")
	_, err = t.DB.Exec(sql)
	if err != nil {
		log.Error(err)
		return err
	}
	meta, err := parseTableMeta(string(schema))
	if err != nil {
		log.Error(err)
		return err
	}
	t.Meta = *meta
	return nil
}

func (t *Table) initRecover() error {
	r, err := rver.New(fmt.Sprintf("recover%d", t.ID))
	if err != nil {
		return err
	}
	err = r.Load()
	if err != nil {
		return err
	}
	t.Recover = r
	return nil
}
