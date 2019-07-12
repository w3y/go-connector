package mysql

import "github.com/spf13/viper"

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

const (
	MaxOpenConn = 100
	MaxIdleConn = 10
)

func NewDefaultConnection() *Connection {
	maxOpenConn := viper.GetInt("mysql.max_open_conn")
	if maxOpenConn <= 0 {
		maxOpenConn = MaxOpenConn
	}

	maxIdleConn := viper.GetInt("mysql.max_idle_conn")
	if maxIdleConn <= 0 {
		maxIdleConn = MaxIdleConn
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
