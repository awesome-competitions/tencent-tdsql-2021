package database

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"time"
)

type DB struct {
	db  *sql.DB
	set []string
}

func New(ip string, port int, user, pwd string) (*DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?", user, pwd, ip, port))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(60 * time.Second)
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(64)

	res, err := db.Query("/*proxy*/ show status")
	if err != nil {
		return nil, err
	}
	var set []string
	for res.Next() {
		name := ""
		value := ""
		err = res.Scan(&name, &value)
		if err != nil {
			return nil, err
		}
		if name == "set" {
			set = strings.Split(value, ",")
		}
	}
	return &DB{
		db:  db,
		set: set,
	}, nil
}

func (d *DB) Exec(sql string, args ...interface{}) (sql.Result, error) {
	return d.db.Exec(sql, args...)
}

func (d *DB) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	return d.db.Query(sql, args...)
}

func (d *DB) Begin() (*sql.Tx, error) {
	return d.db.Begin()
}

func (d *DB) Set() []string {
	return d.set
}
