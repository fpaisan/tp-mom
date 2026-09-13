package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrMessageMiddlewareConnect = errors.New("message middleware: connect error")
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, channel, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}
	middleware, err := m.NewWorkQueueMiddleware(queueName, conn, channel)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}

func connect(connectionSettings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareConnect
	}
	channel, err := conn.Channel()
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			closeErr = ErrMessageMiddlewareConnect
		}
		err = errors.Join(err, closeErr)
		return nil, nil, err
	}
	return conn, channel, nil
}
