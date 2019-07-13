package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	utils "github.com/w3y/go-commons"
	"github.com/w3y/go-connector/pb"
	"math"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	RabbitProducerTimeout = 10000
	AppId                 = ""
)

var (
	ErrSendToClosedProducer = errors.New("send to closed producer...exiting")
	MaxThread               = runtime.NumCPU()
)

func init() {
	AppId = os.Getenv("HOSTNAME")
}

type BufferringData struct {
	data       []byte
	exchName   string
	routingKey string
	mandatory  bool
	immediate  bool
}

type messageInfo struct {
	msg        []byte
	routingKey string
	xchName    string
	mandatory  bool
	immediate  bool
	retries    int64
}

type ProducerInterface interface {
	PublishSimple(exchName string, data []byte) (err error)
	PublishRouting(exchName, routingKey string, data []byte) (err error)
}

type Producer struct {
	conn          *amqp.Connection
	done          chan error
	config        *RabbitMqConfiguration
	status        bool
	retries       int
	messages      chan *messageInfo
	closed        int32
	inputCounter  int64
	outputCounter int64
	errorCounter  int64
	ticker        *time.Ticker

	maxThread int
	appId     string
}

func NewRMQProducerFromConf(conf *RabbitMqConfiguration,
	internalQueueSize int,
	retries int) *Producer {
	if len(conf.URI) == 0 {
		return nil
	}
	return &Producer{
		config:   conf,
		done:     make(chan error),
		status:   false,
		retries:  retries,
		messages: make(chan *messageInfo, internalQueueSize),
		ticker:   time.NewTicker(2 * time.Minute),
	}
}

func NewRMQProducerFromDefConf() *Producer {
	uri := viper.GetString("rabbitmq.uri")
	retries := viper.GetInt64("rabbitmq.retries")
	internalQueueSize := viper.GetInt64("rabbitmq.internal_queue_size")
	if len(uri) == 0 {
		return nil
	}
	conf := &RabbitMqConfiguration{
		URI: uri,
	}

	return &Producer{
		config:   conf,
		done:     make(chan error),
		status:   false,
		retries:  int(retries),
		messages: make(chan *messageInfo, internalQueueSize),
		ticker:   time.NewTicker(2 * time.Minute),
	}

}

func NewRMQProducerFConfig(uri string, retries, internalQueueSize int64,
	co *models.RmqOutputConf) *Producer {
	if len(uri) == 0 {
		return nil
	}
	conf := &RabbitMqConfiguration{
		ExchangeConfig: *convertToRmqConf(co.Exch),
		URI:            uri,
	}

	return &Producer{
		config:   conf,
		done:     make(chan error),
		status:   false,
		retries:  int(retries),
		messages: make(chan *messageInfo, internalQueueSize),
		ticker:   time.NewTicker(2 * time.Minute),
	}
}

func NewRMQProducer(
	uri,
	exchange,
	exchangeType string,
	internalQueueSize,
	retries int) *Producer {
	if len(uri) == 0 {
		return nil
	}
	defaultConfig := DefaultRmqConfiguration(uri, exchange, "")
	defaultConfig.ExchangeConfig.Type = exchangeType

	return &Producer{
		config:   defaultConfig,
		done:     make(chan error),
		status:   false,
		retries:  retries,
		messages: make(chan *messageInfo, internalQueueSize),
		ticker:   time.NewTicker(2 * time.Minute),
	}
}

func (p *Producer) reconnect() error {
	p.status = false
	time.Sleep(20 * time.Second)
	if err := p.connectQueue(); err != nil {
		return err
	}

	return nil
}

func (p *Producer) Connect() error {
	p.maxThread = 1
	return p.connectQueue()
}

func (p *Producer) ConnectMulti(numThread int) error {
	p.maxThread = numThread
	return p.connectQueue()
}

func (p *Producer) connectQueue() error {
	var err error

	if p.config == nil {
		err = errors.New("missing rabbitmq configuration")
		return err
	}

	p.conn, err = amqp.DialConfig(p.config.URI, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, _defaultNetworkTimeoutInSec*time.Second)
		},
	})

	if err != nil {
		return fmt.Errorf("Dial error: %s", err)
	}

	go func() {
		// Waits here for the channel to be closed
		closed := <-p.conn.NotifyClose(make(chan *amqp.Error))

		if closed != nil {
			// Let Handle know it's not time to reconnect
			// ensure goroutine go to end in every case
			select {
			case p.done <- errors.New("channel closed"):
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
	p.status = true

	return nil
}

func (p *Producer) DeclareExchWithDefaultConfig(xchName, xchType string) error {
	xch := RmqConfig{Name: xchName, Type: xchType, Durable: true}
	return p.DeclareSpecificExch(&xch)
}

func (p *Producer) DeclareSpecificExch(xch *RmqConfig) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(
		xch.Name,
		xch.Type,
		xch.Durable,
		xch.AutoDelete,
		xch.Internal,
		xch.NoWait,
		xch.Others,
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	return nil
}

func (p *Producer) DeclareExch() error {
	return p.DeclareSpecificExch(&p.config.ExchangeConfig)
}

func (p *Producer) DeclareSpecificExchN(xch *models.RmqExchQueueInfo) error {
	nxch := convertToRmqConf(xch)
	return p.DeclareSpecificExch(nxch)
}

func (p *Producer) publish(c *amqp.Channel, mi *messageInfo) error {
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        mi.msg,
		Timestamp:   time.Now(),
		AppId:       AppId,
	}
	err := c.Publish(mi.xchName, mi.routingKey, mi.mandatory, mi.immediate, msg)
	if err != nil {
		atomic.AddInt64(&(p.errorCounter), 1)
		err = fmt.Errorf("Cannot publish message to exchange, %v - %v - %v - %v - %v. %v",
			mi.xchName, mi.routingKey, mi.mandatory, mi.immediate, string(mi.msg), err)
		return err
	} else if mi.retries > 0 {
		atomic.AddInt64(&p.errorCounter, -mi.retries)
	}
	atomic.AddInt64(&(p.outputCounter), 1)

	return nil
}
func (p *Producer) PublishMessage(m amqp.Delivery) (err error) {
	if !p.IsClosed() {
		err = p.publishWithTimeout(&messageInfo{msg: m.Body, xchName: m.Exchange, routingKey: m.RoutingKey})
	} else {
		err = ErrSendToClosedProducer
	}
	atomic.AddInt64(&(p.inputCounter), 1)
	return
}

func (p *Producer) PublishProtoSimple(exchName string, msg proto.Message) error {
	b, err := utils.EncodeProto(msg)
	if err != nil {
		return err
	}
	return p.PublishSimple(exchName, b)
}

func (p *Producer) PublishProtoRouting(exchName, routingKey string, msg proto.Message) error {
	b, err := utils.EncodeProto(msg)
	if err != nil {
		return err
	}
	return p.PublishRouting(exchName, routingKey, b)
}

func (p *Producer) PublishProtoWithOption(exchName, key string,
	mandatory, immediate bool, msg proto.Message) error {
	b, err := utils.EncodeProto(msg)
	if err != nil {
		return err
	}
	return p.PublishWithOption(exchName, key, mandatory, immediate, b)
}

func (p *Producer) PublishSimple(exchName string, data []byte) (err error) {
	if data == nil {
		return
	}
	if !p.IsClosed() {
		err = p.publishWithTimeout(&messageInfo{msg: data, xchName: exchName, routingKey: ""})
	} else {
		err = ErrSendToClosedProducer
	}
	atomic.AddInt64(&(p.inputCounter), 1)
	return
}

func (p *Producer) publishWithTimeout(mess *messageInfo) error {
	select {
	case p.messages <- mess:
	case <-time.After(time.Duration(RabbitProducerTimeout) * time.Millisecond):
		return errors.New("publish message to rabbbit timeout")
	}

	return nil
}

func (p *Producer) PublishRouting(exchName, routingKey string, data []byte) (err error) {
	if data == nil {
		return
	}
	if !p.IsClosed() {
		err = p.publishWithTimeout(&messageInfo{msg: data, xchName: exchName, routingKey: routingKey})
	} else {
		err = ErrSendToClosedProducer
	}
	atomic.AddInt64(&(p.inputCounter), 1)
	return
}

func (p *Producer) PublishWithOption(exchName, key string,
	mandatory, immediate bool, data []byte) (err error) {
	if data == nil {
		return
	}
	if !p.IsClosed() {
		err = p.publishWithTimeout(&messageInfo{msg: data, xchName: exchName, routingKey: key, mandatory: mandatory, immediate: immediate})
	} else {
		err = ErrSendToClosedProducer
	}
	atomic.AddInt64(&(p.inputCounter), 1)
	return
}

func (p *Producer) Start() {
	stopTicker := make(chan struct{})
	var countClose int64 = 0
	go func() {
		for {
			select {
			case <-p.ticker.C:
				fmt.Printf(`{"type": "producer", "counter": {"input": %v, "output_success": %v, "error": %v}}`,
					p.InputCount(), p.OutputCount(), p.ErrorCount())
			case <-stopTicker:
				n := atomic.AddInt64(&countClose, 1)
				if n >= int64(p.maxThread) {
					return
				}
			}
		}
	}()

	configTimeout := viper.GetInt("rabbitmq.producer.timeout")
	if configTimeout > 0 {
		RabbitProducerTimeout = configTimeout
	}

	fmt.Printf("Running producer with %v goroutines", p.maxThread)
	m := &sync.Mutex{}
	for i := 0; i < p.maxThread; i++ {
		go func(id int) {
			m.Lock()
			c, err := p.conn.Channel()
			for err != nil {
				time.Sleep(10 * time.Millisecond)
				c, err = p.conn.Channel()
			}
			m.Unlock()
			fmt.Printf("Got channel for %v", id)
			for {
				select {
				case msg := <-p.messages:
					if msg == nil {
						fmt.Printf("Got nil on %v. Breaking...", id)
						stopTicker <- struct{}{}
						defer c.Close()
						return
					}
					err := p.publish(c, msg)
					if err != nil {
						// maybe channel is dead, get new one
						c.Close()
						m.Lock()
						c, err = p.conn.Channel()
						for err != nil {
							time.Sleep(100 * time.Millisecond)
							c, err = p.conn.Channel()
						}
						m.Unlock()
						msg.retries++
						go func() {
							p.publishWithTimeout(msg)
						}()
					}
				}
			}
		}(i)
	}

	go func() {
		var err error
		for {
			// Go into reconnect loop when
			// c.done is passed non nil values
			if err = <-p.done; err != nil {
				if strings.Contains(err.Error(), "channel closed") && !p.IsClosed() { // reconnect case
					p.status = false
					err = p.reconnect()
					retry := 0
					var base = 100
					step := 10
					exp := 2
					for err != nil {
						time.Sleep(time.Duration(base+int(math.Pow(float64(step), float64(exp)))) * time.Millisecond)
						// Very likely chance of failing
						// should not cause worker to terminate
						retry++
						if retry > p.retries {
							panic(fmt.Errorf("Cannot retry connection after %v times", p.retries))
						}
						err = p.reconnect()
					}
				} else { // stop case
					p.conn.Close()
					return
				}
			}
		}
	}()
}

func (p *Producer) Close() {
	atomic.StoreInt32(&(p.closed), 1)
	time.Sleep(1 * time.Second)
	close(p.messages)
	p.done <- errors.New("stop rabbitmq producer")
	p.conn.Close()
	p.ticker.Stop()
}

func (p *Producer) IsClosed() bool {
	return atomic.LoadInt32(&(p.closed)) == 1
}

func (p *Producer) OutputCount() int64 {
	return atomic.LoadInt64(&(p.outputCounter))
}
func (p *Producer) InputCount() int64 {
	return atomic.LoadInt64(&(p.inputCounter))
}

func (p *Producer) ErrorCount() int64 {
	return atomic.LoadInt64(&(p.errorCounter))
}
