package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type Mysql struct {
	*sql.DB
}

func NewConnection(conn *Connection) (*Mysql, error) {
	var err error
	var db *sql.DB
	settings := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=%s&parseTime=%t",
		conn.User, conn.Password,
		conn.Protocol,
		conn.Host, conn.Port,
		conn.DatabaseName,
		conn.Charset, conn.ParseTime,
	)
	if utils.IsStringNotEmpty(conn.Others) {
		settings = fmt.Sprintf("%s&%s", settings, conn.Others)
	}
	db, err = sql.Open("mysql", settings)
	if err != nil {
		return nil, err
	}
	if conn.MaxOpenConn > 0 {
		db.SetMaxOpenConns(conn.MaxOpenConn)
	}
	if conn.MaxIdleConn > 0 && conn.MaxIdleConn < conn.MaxOpenConn {
		db.SetMaxIdleConns(conn.MaxIdleConn)
	}
	db.SetConnMaxLifetime(3600 * time.Second)

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &Mysql{db}, nil
}


func (c *Mysql) GetDB() *sql.DB {
	return c.DB
}


func (c *Mysql) Close() {
	if c == nil {
		return
	}
	err := c.DB.Close()
	if err != nil {
		//TODO: loggger
	}
}
