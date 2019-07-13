package rabbitmq

import (
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	utils "github.com/w3y/go-commons"
)


const (
	_defaultQueueSize           = 1000
	_defaultNetworkTimeoutInSec = 25
	_defaultRetries = 10
)


type RmqConfig struct {
	Name       string
	Type       string
	AutoDelete bool
	Durable    bool
	Internal   bool
	Exclusive  bool
	NoWait     bool
	Others     map[string]interface{}
}

func censorURI(uri string) string {
	rbUri, err := amqp.ParseURI(uri)
	if err != nil {
		return ""
	}

	rbUri.Password = utils.CensorString(rbUri.Password)
	return rbUri.String()
}

type RabbitMqConfiguration struct {
	// amqp://user:pass@host:port/vhost?heartbeat=10&connection_timeout=10000&channel_max=100
	URI string
	ExchangeConfig RmqConfig
	QueueConfig    RmqConfig
}

func DefaultRmqConfiguration(uri, exchangeName, queueName string) *RabbitMqConfiguration {
	return &RabbitMqConfiguration{
		uri,
		RmqConfig{exchangeName, "direct", false, true, false, false, false, make(map[string]interface{})},
		RmqConfig{queueName, "direct", false, true, false, false, false, make(map[string]interface{})},
	}
}

func DefaultRMqConfFromConfig() *RabbitMqConfiguration {
	uri := viper.GetString("rabbitmq.uri")
	exch := viper.GetString("rabbitmq.exchange")
	exchType := viper.GetString("rabbitmq.exchange_type")
	xchAutoDelete := viper.GetBool("rabbitmq.exchange_autodelete")
	xchDurable := viper.GetBool("rabbitmq.exchange_durable")

	// queue
	queue := viper.GetString("rabbitmq.queue")
	qType := viper.GetString("rabbitmq.queue_type")
	qAutoDelete := viper.GetBool("rabbitmq.queue_autodelete")
	qDurable := viper.GetBool("rabbitmq.queue_durable")

	exchConf := RmqConfig{exch, exchType, xchAutoDelete, xchDurable, false, false, false, make(map[string]interface{})}
	queueConf := RmqConfig{queue, qType, qAutoDelete, qDurable, false, false, false, make(map[string]interface{})}
	return &RabbitMqConfiguration{
		URI:            uri,
		ExchangeConfig: exchConf,
		QueueConfig:    queueConf,
	}
}

func LoadQueueConfig() *RmqConfig {
	queue := viper.GetString("rabbitmq.queue")
	qType := viper.GetString("rabbitmq.queue_type")
	qAutoDelete := viper.GetBool("rabbitmq.queue_autodelete")
	qDurable := viper.GetBool("rabbitmq.queue_durable")
	return &RmqConfig{queue, qType, qAutoDelete, qDurable, false, false, false, make(map[string]interface{})}
}

func LoadExchConfig() *RmqConfig {
	exch := viper.GetString("rabbitmq.exchange")
	exchType := viper.GetString("rabbitmq.exchange_type")
	xchAutoDelete := viper.GetBool("rabbitmq.exchange_autodelete")
	xchDurable := viper.GetBool("rabbitmq.exchange_durable")

	return &RmqConfig{exch, exchType, xchAutoDelete, xchDurable, false, false, false, make(map[string]interface{})}
}

func LoadOtherConfigParams() map[string]interface{} {
	internalQueueSize := viper.GetInt("rabbitmq.internal_queue_size")
	retries := viper.GetInt("rabbitmq.retries")
	uri := viper.GetString("rabbitmq.uri")
	numThread := viper.GetInt("rabbitmq.num_thread")
	m := make(map[string]interface{})
	m["internal_queue_size"] = internalQueueSize
	m["retries"] = retries
	m["uri"] = uri
	m["num_thread"] = numThread
	return m
}