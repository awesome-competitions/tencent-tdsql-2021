package table

import (
	"github.com/ainilili/tdsql-competition/database"
	"github.com/ainilili/tdsql-competition/file"
	"github.com/ainilili/tdsql-competition/parser"
	"github.com/ainilili/tdsql-competition/util"
	"io/ioutil"
	"strings"
)

func ParseTables(db *database.DB, dataPath string) ([]*Table, error) {
	dataSourceFiles, err := ioutil.ReadDir(dataPath)
	if err != nil {
		return nil, err
	}
	tables := make([]*Table, 0)
	tableId := 1
	tableMap := map[string]*Table{}
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
			database := util.ParseName(databaseFile.Name())
			dataFiles := map[string]*file.File{}
			schemaFiles := map[string]*file.File{}
			fileKeys := make([]string, 0)
			for _, tableFile := range tableFiles {
				f, err := file.New(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()))
				if err != nil {
					return nil, err
				}
				fileKey := tableFile.Name()[:len(tableFile.Name())-4]
				fileKeys = append(fileKeys, fileKey)
				if strings.HasSuffix(tableFile.Name(), ".csv") {
					dataFiles[fileKey] = f
				} else {
					schemaFiles[fileKey] = f
				}
			}
			for _, k := range fileKeys {
				schema := schemaFiles[k]
				data := dataFiles[k]
				tableName := util.ParseName(data.Name())
				tableKey := database + ":" + tableName
				t, ok := tableMap[tableKey]
				if !ok {
					t = &Table{
						ID:       tableId,
						Name:     tableName,
						Database: database,
						Data:     make([]Data, 0),
						Schema:   schema,
						DB:       db,
					}
					tableId++
					tables = append(tables, t)
					tableMap[tableKey] = t
				}
				t.Data = append(t.Data, Data{
					Data:       data,
					DataSource: dataSource,
				})
			}
		}
	}
	return tables, nil
}

func parseTableMeta(sql string) (*Meta, error) {
	stmt := parser.ParseTableStmt(sql)
	cols := make([]string, 0)
	colsIndex := map[string]int{}
	colsType := map[string]Type{}
	for i, col := range stmt.Cols {
		cols = append(cols, col.Name)
		colsIndex[col.Name] = i
		colsType[col.Name] = SqlTypeMapping[col.Type]
	}
	return &Meta{
		Keys:      stmt.Keys,
		Cols:      cols,
		ColsIndex: colsIndex,
		ColsType:  colsType,
	}, nil
}
