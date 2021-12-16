package sync

import (
    "github.com/ainilili/tdsql-competition/db"
    "github.com/ainilili/tdsql-competition/model"
    _ "github.com/pingcap/tidb/types/parser_driver"
)

func Sync(db *db.DB, table *model.Table) error{
    return nil
}
