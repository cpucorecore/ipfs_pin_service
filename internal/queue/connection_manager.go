package queue

import (
	"context"
	"github.com/cpucorecore/ipfs_pin_service/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type ConnectionManager interface {
	GetConnection() *amqp.Connection
	GetChannel() (*amqp.Channel, error)
}

type connectionManager struct {
	url        string
	lock       sync.RWMutex
	connection *amqp.Connection
}

func NewConnectionManager(url string) ConnectionManager {
	return &connectionManager{
		url: url,
	}
}

func (m *connectionManager) getConnection() *amqp.Connection {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.connection
}

func (m *connectionManager) setConnection(conn *amqp.Connection) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.connection = conn
}

func (m *connectionManager) monitorConnectionPassive(ctx context.Context, connection *amqp.Connection, notifyClose chan struct{}) {
	notifyCh := connection.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case <-ctx.Done():
			return
		case <-notifyClose:
			err := <-notifyCh
			log.Log.Sugar().Warn("connection is closed(passive notify)")
			if err != nil {
			}
			notifyClose <- struct{}{}
		}
	}
}

func (m *connectionManager) monitorConnectionActive(ctx context.Context, connection *amqp.Connection, notifyClose chan struct{}) {
	const checkInterval = 10 * time.Second
	tick := time.Tick(checkInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			if connection.IsClosed() {
				log.Log.Sugar().Warnf("connection is closed(active check)")
				notifyClose <- struct{}{}
				return
			}
			log.Log.Sugar().Info("connection alive")
		}
	}
}

func (m *connectionManager) monitorConnection(connection *amqp.Connection) {
	closeNotifyCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go m.monitorConnectionPassive(ctx, connection, closeNotifyCh)
	go m.monitorConnectionActive(ctx, connection, closeNotifyCh)

	for {
		select {
		case <-closeNotifyCh:
			cancel()
			connection.Close()
			m.setConnection(nil)
		}
	}
}

func (m *connectionManager) mustConnect() *amqp.Connection {
	const maxDelay = 30 * time.Second
	var retryDelay = time.Second
	for {
		time.Sleep(retryDelay)
		connection, err := amqp.Dial(m.url)
		if err == nil {
			if connection == nil || connection.IsClosed() {
				panic("shouldn't happen")
			}

			go m.monitorConnection(connection)
			return connection
		}

		retryDelay *= 2
		if retryDelay > maxDelay {
			retryDelay = maxDelay
		}
	}
}

func (m *connectionManager) GetConnection() *amqp.Connection {
	connection := m.getConnection()
	if connection != nil {
		return connection
	}

	connection = m.mustConnect()
	m.setConnection(connection)
	go m.monitorConnection(connection)
	return connection
}

func (m *connectionManager) GetChannel() (*amqp.Channel, error) {
	return m.GetConnection().Channel()
}

var _ ConnectionManager = &connectionManager{}
