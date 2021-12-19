package database

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type DB struct {
	db *sql.DB
}

func New(ip string, port int, user, pwd string) (*DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, pwd, ip, port))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(60 * time.Second)
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)
	return &DB{
		db: db,
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
