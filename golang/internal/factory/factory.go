package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, channel, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}
	middleware, err := NewWorkQueueMiddleware(queueName, conn, channel)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, channel, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}
	middleware, err := NewExchangeMiddleware(exchange, keys, conn, channel)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

func connect(connectionSettings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}
	channel, err := conn.Channel()
	if err != nil {
		closeErr := closeConnection(conn)
		if errors.Is(err, amqp.ErrClosed) {
			if closeErr != nil {
				return nil, nil, errors.Join(m.ErrMessageMiddlewareDisconnected, closeErr)
			}
			return nil, nil, m.ErrMessageMiddlewareDisconnected
		}
		if closeErr != nil {
			return nil, nil, errors.Join(m.ErrMessageMiddlewareMessage, closeErr)
		}
		return nil, nil, m.ErrMessageMiddlewareMessage
	}
	return conn, channel, nil
}

func closeConnection(conn *amqp.Connection) error {
	if !conn.IsClosed() {
		connErr := conn.Close()
		if connErr != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	return nil
}
