package main

import (
	"flag"
	"fmt"
	"github.com/ainilili/tdsql-competition/binlog"
)

var src_a_ip *string
var src_a_port *uint
var src_a_user *string
var src_a_password *string
var src_a_gtid *string

func init() {
	src_a_ip = flag.String("src_a_ip", "sh-cdb-jcp0bqe8.sql.tencentcdb.com", "")
	src_a_port = flag.Uint("src_a_port", 58757, "")
	src_a_user = flag.String("src_a_user", "nico", "")
	src_a_password = flag.String("src_a_password", "Niconico2021@", "")
	src_a_gtid = flag.String("src_a_gtid", "958da3d3-93ab-11ec-9d0f-bc97e1e97d80:13", "")
	flag.Parse()
	fmt.Println("input：", src_a_ip, src_a_port, src_a_user, src_a_password, src_a_gtid)
}

func main() {
	binlog.Listen(*src_a_ip, uint16(*src_a_port), *src_a_user, *src_a_password, *src_a_gtid)
}
