package parser

import (
    "strings"
)

type TableStmt struct {
    Name string
    Cols []Column
    Keys []string
}

type Column struct {
    Name string
    Type string
}

func ParseTableStmt(sql string) *TableStmt{
    sql = strings.ToLower(sql)
    lines := strings.Split(sql, "\n")
    stmt := &TableStmt{}
    for _, line := range lines{
        line = strings.TrimSpace(line)
        subs := strings.Split(line, " ")
        if strings.HasPrefix(line, "create table"){
            stmt.Name = strings.ReplaceAll(subs[5], "`", "")
        }else if strings.HasPrefix(line, "`") {
            name := strings.ReplaceAll(subs[0], "`", "")
            t := strings.Split(subs[1], "(")[0]
            stmt.Cols = append(stmt.Cols, Column{
                Name:name,
                Type:t,
            })
        }else if strings.HasPrefix(line, "primary key") {
            keys := strings.Split(strings.ReplaceAll(subs[2][1:len(subs[2]) - 1], "`", ""), ",")
            stmt.Keys = keys
        }
    }
    return stmt
}

