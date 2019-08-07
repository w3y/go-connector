package redis

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

const (
	Sentinel = "sentinel"
	Cluster  = "cluster"

	DefaultPoolSize     = 100
	DefaultReadTimeout  = 1000
	DefaultWriteTimeout = 1000
)

var (
	ErrorMissingRedisAddress = errors.New("missing redis address")
)

type Connection interface {
	BuildClient() (redis.UniversalClient, error)
}

func NewRedisConfig(add string, db int) Connection {
	return &SingleConnection{
		address:      add,
		db:           db,
		poolSize:     DefaultPoolSize,
		readTimeout:  viper.GetInt("redis.read_timeout"),
		writeTimeout: viper.GetInt("redis.write_timeout"),
	}
}
