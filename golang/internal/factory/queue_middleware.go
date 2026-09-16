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
	_, err := queueDeclare(channel, queueName, WORK_QUEUE_DURABILITY, WORK_QUEUE_EXCLUSIVITY)
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
	err := checkResources(q.isConsuming, q.Connection, q.Channel)
	if err != nil {
		return err
	}
	q.isConsuming = true
	if err := consumeMessages(q.QueueName, q.Channel, q.QueueName, callbackFunc); err != nil {
		q.isConsuming = false
		return err
	}
	if q.isConsuming {
		return middleware.ErrMessageMiddlewareDisconnected
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
	q.isConsuming = false
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
	if err := closeResources(q.Connection, q.Channel); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
