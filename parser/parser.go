package parser

import (
    "errors"
    "github.com/ainilili/tdsql-competition/file"
    "github.com/ainilili/tdsql-competition/model"
    "github.com/ainilili/tdsql-competition/util"
    "github.com/pingcap/parser"
    "github.com/pingcap/parser/ast"
    "io/ioutil"
    "strings"
)

func ParseTables(dataPath string) ([]*model.Table, error){
    dataSourceFiles, err := ioutil.ReadDir(dataPath)
    if err != nil {
        return nil, err
    }
    tables := make([]*model.Table, 0)
    tableMap := map[string]*model.Table{}
    for _, dataSourceFile := range dataSourceFiles {
        databaseFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name()))
        if err != nil{
            return nil, err
        }
        dataSource := util.ParseName(dataSourceFile.Name())
        for _, databaseFile := range databaseFiles {
            tableFiles, err := ioutil.ReadDir(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name()))
            if err != nil{
                return nil, err
            }
            database := util.ParseName(databaseFile.Name())
            dataFiles := map[string]*file.File{}
            schemaFiles := map[string]*file.File{}
            for _, tableFile := range tableFiles{
                f, err := file.New(util.AssemblePath(dataPath, dataSourceFile.Name(), databaseFile.Name(), tableFile.Name()))
                if err != nil{
                    return nil, err
                }
                if strings.HasSuffix(tableFile.Name(), ".csv") {
                    dataFiles[tableFile.Name()[:len(tableFile.Name())- 4]] = f
                }else{
                    schemaFiles[tableFile.Name()[:len(tableFile.Name())- 4]] = f
                }
            }
            for k, data := range dataFiles{
                schema := schemaFiles[k]
                tableName := util.ParseName(data.Name())
                tableKey := database + ":" + tableName
                table, ok := tableMap[tableKey]
                if ! ok{
                    table = &model.Table{
                        Name: tableName,
                        Database: database,
                        Data: make([]model.TableData, 0),
                        Schema: schema,
                    }
                    tables = append(tables, table)
                    tableMap[tableKey] = table
                }
                table.Data = append(table.Data, model.TableData{
                    Data: data,
                    DataSource: dataSource,
                })
            }
        }
    }
    return tables, nil
}

func ParseTableMeta(sql string) (*model.TableMeta, error){
    p := parser.New()
    stmt, _, err := p.Parse(sql, "", "")
    if err != nil{
        return nil, err
    }
    tableStmt, ok := stmt[0].(*ast.CreateTableStmt)
    if ! ok {
        return nil, errors.New("sql schema invalid. ")
    }
    cols := make([]string, 0)
    keys := make([]string, 0)
    for _, col := range tableStmt.Cols{
        cols = append(cols, col.Name.String())
    }
    for _, constraint := range tableStmt.Constraints {
        for _, key := range constraint.Keys{
            keys = append(keys, key.Column.Name.String())
        }
    }
    return &model.TableMeta{
        Keys: keys,
        Cols: cols,
    }, nil
}

