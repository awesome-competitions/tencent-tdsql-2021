package parser

import "testing"

func TestParseTableStmt(t *testing.T) {
    sql := "CREATE TABLE if not exists `2` (\n  `id` bigint(20) unsigned NOT NULL,\n  `a` float NOT NULL DEFAULT '0',\n  `b` char(32) NOT NULL DEFAULT '',\n  `updated_at` datetime NOT NULL DEFAULT '2021-12-12 00:00:00',\n  PRIMARY KEY (`id`,`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    stmt := ParseTableStmt(sql)
    t.Log(stmt.Name)
    t.Log(stmt.Keys)
    t.Log(stmt.Cols)
}


