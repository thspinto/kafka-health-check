package check

import (
	"time"

	kafka "github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

// BrokerConnection represents a connection to the Kafka broker
type BrokerConnection interface {
	Dial(nodeAddresses []string, conf kafka.Config) error

	Consumer(conf kafka.Config) (kafka.Consumer, error)

	Producer(conf kafka.Config) kafka.SyncProducer

	Metadata() (*kafka.MetadataResponse, error)

	Close()
}

// actual implementation of the Kafka broker connection based on optiopay/kafka.
type kafkaBrokerConnection struct {
	broker        *kafka.Broker
	brokerAddress string
}

func (connection *kafkaBrokerConnection) Dial(brokerAddress string, conf *kafka.Config) error {
	broker := kafka.NewBroker(brokerAddress)
	err := broker.Open(conf)
	if err != nil {
		return err
	}
	connection.broker = broker
	connection.brokerAddress = brokerAddress
	return nil
}

func (connection *kafkaBrokerConnection) Consumer(conf *kafka.Config) (kafka.Consumer, error) {
	return kafka.NewConsumer([]string{connection.brokerAddress}, conf)
}

func (connection *kafkaBrokerConnection) Producer(conf *kafka.Config) (kafka.SyncProducer, error) {
	return kafka.NewSyncProducer([]string{connection.brokerAddress}, conf)
}

func (connection *kafkaBrokerConnection) Metadata() (*kafka.MetadataResponse, error) {
	return connection.broker.GetMetadata(&kafka.MetadataRequest{})
}

func (connection *kafkaBrokerConnection) Close() {
	connection.broker.Close()
}

// ZkConnection represents a connection to a ZooKeeper ensemble
type ZkConnection interface {
	Connect(servers []string, sessionTimeout time.Duration) (<-chan zk.Event, error)
	Close()
	Exists(path string) (bool, *zk.Stat, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Children(path string) ([]string, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
}

// Actual implementation based on samuel/go-zookeeper/zk
type zkConnection struct {
	connection *zk.Conn
}

type zkNullLogger struct {
}

func (zkNullLogger) Printf(string, ...interface{}) {}

func (zkConn *zkConnection) Connect(servers []string, sessionTimeout time.Duration) (<-chan zk.Event, error) {
	loggerOption := func(c *zk.Conn) {
		c.SetLogger(zkNullLogger{})
	}
	connection, events, err := zk.Connect(servers, sessionTimeout, loggerOption)
	zkConn.connection = connection
	return events, err
}

func (zkConn *zkConnection) Close() {
	zkConn.connection.Close()
}

func (zkConn *zkConnection) Exists(path string) (bool, *zk.Stat, error) {
	return zkConn.connection.Exists(path)
}

func (zkConn *zkConnection) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	return zkConn.connection.Set(path, data, version)
}

func (zkConn *zkConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return zkConn.connection.Create(path, data, flags, acl)
}

func (zkConn *zkConnection) Children(path string) ([]string, *zk.Stat, error) {
	return zkConn.connection.Children(path)
}

func (zkConn *zkConnection) Get(path string) ([]byte, *zk.Stat, error) {
	return zkConn.connection.Get(path)
}
