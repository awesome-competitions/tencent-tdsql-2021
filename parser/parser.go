package parser

import (
	"bytes"
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/log"
	"github.com/ainilili/tdsql-competition/model"
	"github.com/ainilili/tdsql-competition/util"
	"io/ioutil"
	"os"
	"strings"
)

type TableStmt struct {
	Name        string
	Cols        []Column
	PrimaryKeys []string
	Keys        []string
}

type Column struct {
	Name         string
	Type         string
	Required     bool
	DefaultValue string
}

func ParseTableStmt(sql string) *TableStmt {
	sql = strings.ToLower(sql)
	lines := strings.Split(sql, "\n")
	stmt := &TableStmt{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		subs := strings.Split(line, " ")
		if strings.HasPrefix(line, "create table") {
			stmt.Name = strings.ReplaceAll(subs[5], "`", "")
		} else if strings.HasPrefix(line, "`") {
			name := strings.ReplaceAll(subs[0], "`", "")
			t := strings.Split(subs[1], "(")[0]
			required := strings.Contains(line, "not null")
			defaultValue := bytes.Buffer{}
			defaultValue.WriteString("null")
			if i := strings.Index(line, "default"); i != -1 {
				defaultValue.Reset()
				start := false
				for ; i < len(line); i++ {
					if line[i] == '\'' {
						if start {
							break
						}
						start = true
						continue
					}
					if start {
						defaultValue.WriteByte(line[i])
					}
				}
			}
			stmt.Cols = append(stmt.Cols, Column{
				Name:         name,
				Type:         t,
				Required:     required,
				DefaultValue: defaultValue.String(),
			})
		} else if strings.HasPrefix(line, "primary key") {
			keys := strings.Split(strings.ReplaceAll(subs[2][1:len(subs[2])-1], "`", ""), ",")
			stmt.PrimaryKeys = keys
			stmt.Keys = keys
		} else if strings.HasPrefix(line, "key") {
			keys := strings.Split(strings.ReplaceAll(subs[1][1:len(subs[1])-1], "`", ""), ",")
			stmt.Keys = keys
		}
	}
	return stmt
}

func ParseTableMeta(sql string) model.Meta {
	stmt := ParseTableStmt(sql)
	cols := make([]string, 0)
	colsIndex := map[string]int{}
	colsType := map[string]model.Type{}
	defaultValue := map[string]string{}
	for i, col := range stmt.Cols {
		cols = append(cols, col.Name)
		colsIndex[col.Name] = i
		colsType[col.Name] = model.SqlTypeMapping[col.Type]
		defaultValue[col.Name] = col.DefaultValue
	}
	return model.Meta{
		PrimaryKeys:  stmt.PrimaryKeys,
		Keys:         stmt.Keys,
		Cols:         cols,
		ColsIndex:    colsIndex,
		ColsType:     colsType,
		DefaultValue: defaultValue,
	}
}

func ParseTables(db *database.DB, dataPath string) ([]*model.Table, error) {
	dataSourceFiles, err := ioutil.ReadDir(dataPath)
	if err != nil {
		return nil, err
	}
	tables := make([]*model.Table, 0)
	tableId := 1
	tableMap := map[string]*model.Table{}
	tablesMap := map[string][]*model.Table{}
	for _, dataSourceFile := range dataSourceFiles {
		databaseFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name()))
		if err != nil {
			return nil, err
		}
		dataSource := util.ParseName(dataSourceFile.Name())
		for _, databaseFile := range databaseFiles {
			tableFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name()))
			if err != nil {
				return nil, err
			}
			dbName := util.ParseName(databaseFile.Name())
			dataFiles := map[string]*file.File{}
			schemaFiles := map[string]*file.File{}
			fileKeys := make([]string, 0)
			for _, tableFile := range tableFiles {
				f, err := file.New(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()), os.O_RDONLY)
				if err != nil {
					return nil, err
				}
				fileKey := tableFile.Name()[:len(tableFile.Name())-4]
				if strings.HasSuffix(tableFile.Name(), ".csv") {
					dataFiles[fileKey] = f
					fileKeys = append(fileKeys, fileKey)
				} else {
					schemaFiles[fileKey] = f
				}
			}
			for _, k := range fileKeys {
				schema, err := schemaFiles[k].ReadAll()
				if err != nil {
					log.Error(err)
					return nil, err
				}
				data := dataFiles[k]
				tableName := util.ParseName(data.Name())
				tableKey := dbName + ":" + tableName
				t, ok := tableMap[tableKey]
				if !ok {
					t = &model.Table{
						ID:       tableId,
						Name:     tableName,
						Database: dbName,
						Sources:  make([]model.Source, 0),
						Schema:   string(schema),
						DB:       db,
						Meta:     ParseTableMeta(string(schema)),
					}
					//r, err := rver.New(fmt.Sprintf("recover%d", t.ID))
					//if err != nil {
					//	return nil, err
					//}
					//t.Recover = r
					//for i, c := range t.Meta.Cols {
					//	t.Cols += c
					//	if i != len(t.Meta.Cols)-1 {
					//		t.Cols += ","
					//	}
					//}
					//setRecovers := map[string]*rver.Recover{}
					//for _, set := range db.Sets() {
					//	r, err := rver.New(fmt.Sprintf("recover_offset_%d_%s", t.ID, set))
					//	if err != nil {
					//		return nil, err
					//	}
					//	setRecovers[set] = r
					//}
					//t.SetRecovers = setRecovers
					tableId++
					tables = append(tables, t)
					tablesMap[dbName] = append(tablesMap[dbName], t)
					tableMap[tableKey] = t
				}
				t.Sources = append(t.Sources, model.Source{
					File:       data,
					DataSource: dataSource,
				})
			}
		}
	}
	return tables, nil
}

func distributeTables(tables []*model.Table) []*model.Table {
	tableMap := map[string][]*model.Table{}
	for i, table := range tables {
		tableMap[table.Database] = append(tableMap[table.Database], tables[i])
	}
	newTables := make([]*model.Table, 0)
	for i := range tableMap[tables[0].Database] {
		for _, ts := range tableMap {
			newTables = append(newTables, ts[i])
		}
	}
	return newTables
}
