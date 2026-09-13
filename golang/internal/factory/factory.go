package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	err = channel.Qos(
		PREFETCH_COUNT,
		PREFETCH_SIZE,
		GLOBAL,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}
	return &m.WorkQueueMiddleware{
		QueueName:   queueName,
		Connection:  conn,
		Channel:     channel,
		ConsumerTag: "consumer-" + queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
