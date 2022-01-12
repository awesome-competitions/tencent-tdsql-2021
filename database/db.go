package database

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"time"
)

type DB struct {
	db   *sql.DB
	sets []string
	hash []string
}

func New(ip string, port int, user, pwd string) (*DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?unique_checks=off", user, pwd, ip, port))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(60 * time.Second)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(500)

	res, err := db.Query("show variables")
	if err != nil {
		return nil, err
	}
	for res.Next() {
		name := ""
		value := ""
		err = res.Scan(&name, &value)
		if err != nil {
			return nil, err
		}
		fmt.Println(name, value)
	}

	res, err = db.Query("/*proxy*/ show status")
	if err != nil {
		return nil, err
	}
	sets := make([]string, 0)
	hash := make([]string, 64)
	for res.Next() {
		name := ""
		value := ""
		err = res.Scan(&name, &value)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(name, "hash_range") {
			set := strings.Split(name, ":")[0]
			sets = append(sets, set)
			rg := strings.Split(value, "---")
			left, _ := strconv.ParseInt(rg[0], 10, 64)
			right, _ := strconv.ParseInt(rg[1], 10, 64)
			for i := left; i <= right; i++ {
				hash[i] = set
			}
		}
	}
	return &DB{
		db:   db,
		sets: sets,
		hash: hash,
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

func (d DB) Hash() []string {
	return d.hash
}

func (d DB) Sets() []string {
	return d.sets
}

func (d *DB) GetConn(ctx context.Context) (*sql.Conn, error) {
	return d.db.Conn(ctx)
}
