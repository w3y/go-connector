package redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

type SingleConnection struct {
	network      string
	address      string
	password     string
	db           int
	maxRetries   int
	poolSize     int
	readTimeout  int
	writeTimeout int
}

func (conn *SingleConnection) BuildClient() (redis.UniversalClient, error) {
	if conn.address == "" {
		return nil, ErrorMissingRedisAddress
	}

	if conn.readTimeout == 0 {
		conn.readTimeout = DefaultReadTimeout
	}

	if conn.writeTimeout == 0 {
		conn.writeTimeout = DefaultWriteTimeout
	}

	return redis.NewClient(
		&redis.Options{
			Addr:         conn.address,
			Password:     conn.password, // no password set
			DB:           conn.db,       // use default DB
			PoolSize:     conn.poolSize,
			PoolTimeout:  time.Second * 4,
			ReadTimeout:  time.Millisecond * time.Duration(conn.readTimeout),
			WriteTimeout: time.Millisecond * time.Duration(conn.writeTimeout),
		},
	), nil
}

type SentinelConnection struct {
	masterGroup       string
	sentinelAddresses []string
	password          string
	db                int
	poolSize          int
	readTimeout       int
	writeTimeout      int
}

func (conn *SentinelConnection) BuildClient() (redis.UniversalClient, error) {
	if len(conn.sentinelAddresses) == 0 {
		return nil, ErrorMissingRedisAddress
	}

	masterGroup := conn.masterGroup
	if masterGroup == "" {
		masterGroup = "master"
	}

	if conn.readTimeout == 0 {
		conn.readTimeout = DefaultReadTimeout
	}

	if conn.writeTimeout == 0 {
		conn.writeTimeout = DefaultWriteTimeout
	}

	redisdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterGroup,
		SentinelAddrs: conn.sentinelAddresses,
		Password:      conn.password,
		DB:            conn.db,
		PoolSize:      conn.poolSize,
		PoolTimeout:   time.Second * 4,
		ReadTimeout:   time.Millisecond * time.Duration(conn.readTimeout),
		WriteTimeout:  time.Millisecond * time.Duration(conn.writeTimeout),
	})

	return redisdb, nil
}

func IsInSlot(key string, slot redis.ClusterSlot) bool {
	s := Slot(key)
	return slot.Start <= s && s <= slot.End
}

func GetSlotID(key string, slots []redis.ClusterSlot) string {
	s := Slot(key)
	for k := range slots {
		slot := slots[k]
		if slot.Start <= s && s <= slot.End {
			return fmt.Sprintf("%v-%v", slot.Start, slot.End)
		}
	}
	return ""
}

type RedisInterface interface {
	redis.UniversalClient
	GetClient() redis.UniversalClient
	GetClusterSlots() ([]redis.ClusterSlot, error)
	GetRedisSlot(key string) int
	GetRedisSlotID(key string) string
}

type Redis struct {
	redis.UniversalClient
	Slots       []redis.ClusterSlot
}

func NewConnection(conn Connection) (*Redis, error) {
	var err error
	c, err := conn.BuildClient()
	if err != nil {
		return nil, err
	}
	_, err = c.Ping().Result()
	if err != nil {
		return nil, err
	}
	cs := getClusterInfo(c)

	return getRedis(c, cs), nil
}

func getRedis(c redis.UniversalClient, cs []redis.ClusterSlot) *Redis {
	rd := &Redis{c, cs}
	return rd
}

func getClusterInfo(c redis.UniversalClient) []redis.ClusterSlot {
	var cs = make([]redis.ClusterSlot, 0)
	if ci := c.ClusterInfo(); ci.Err() == nil {
		csr := c.ClusterSlots()
		cs, _ = csr.Result()
	}
	return cs
}

func (br *Redis) Close() error {
	if br != nil {
		return br.UniversalClient.Close()
	}
	return nil
}

func (br *Redis) GetClient() redis.UniversalClient {
	return br.UniversalClient
}

func (br *Redis) GetClusterSlots() ([]redis.ClusterSlot, error) {
	res := br.ClusterSlots()
	return res.Result()
}

func (br *Redis) GetRedisSlot(key string) int {
	return Slot(key)
}

func (br *Redis) GetRedisSlotID(key string) string {
	return GetSlotID(key, br.Slots)
}
