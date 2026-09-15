package factory

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkQueueMiddleware struct {
	QueueName   string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	isConsuming bool
}

func NewWorkQueueMiddleware(queueName string, conn *amqp.Connection, channel *amqp.Channel) (*WorkQueueMiddleware, error) {
	_, err := channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}
	err = channel.Qos(
		PREFETCH_COUNT,
		PREFETCH_SIZE,
		GLOBAL,
	)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}
	return &WorkQueueMiddleware{
		QueueName:  queueName,
		Connection: conn,
		Channel:    channel,
	}, nil
}

func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	if q.isConsuming {
		return middleware.ErrMessageMiddlewareMessage
	}
	if q.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if q.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareMessage
	}
	q.isConsuming = true
	if err := consumeMessages(q.QueueName, q.Channel, q.QueueName, callbackFunc); err != nil {
		q.isConsuming = false
		return err
	}
	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() error {
	if q.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if !q.isConsuming {
		return nil
	}
	return cancelChannel(q.QueueName, q.Channel)

}

func (q *WorkQueueMiddleware) Send(msg middleware.Message) error {
	if q.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if q.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return publish(q.Channel, DEFAULT_EXCHANGE, q.QueueName, msg.Body)
}

func (q *WorkQueueMiddleware) Close() error {
	err := q.Channel.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	err = q.Connection.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
