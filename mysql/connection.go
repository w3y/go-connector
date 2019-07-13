package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	utils "github.com/w3y/go-commons"
	"time"
)

const (
	_maxOpenConn = 100
	_maxIdleConn = 10
)

type Connection struct {
	Host         string
	Port         int
	Protocol     string
	User         string
	Password     string
	DatabaseName string
	Charset      string
	ParseTime    bool
	Others       string
	MaxOpenConn  int
	MaxIdleConn  int
}

func NewDefaultConnectionConfig() *Connection {
	maxOpenConn := viper.GetInt("mysql.max_open_conn")
	if maxOpenConn <= 0 {
		maxOpenConn = _maxOpenConn
	}

	maxIdleConn := viper.GetInt("mysql.max_idle_conn")
	if maxIdleConn <= 0 {
		maxIdleConn = _maxIdleConn
	}
	return &Connection{
		Host:         viper.GetString("mysql.host"),
		Port:         viper.GetInt("mysql.port"),
		Protocol:     viper.GetString("mysql.protocol"),
		User:         viper.GetString("mysql.user"),
		Password:     viper.GetString("mysql.password"),
		DatabaseName: viper.GetString("mysql.database_name"),
		Charset:      viper.GetString("mysql.charset"),
		ParseTime:    viper.GetBool("mysql.parse_time"),
		Others:       viper.GetString("mysql.others"),
		MaxOpenConn:  maxOpenConn,
		MaxIdleConn:  maxIdleConn,
	}
}

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
	if !utils.IsStringEmpty(conn.Others) {
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
