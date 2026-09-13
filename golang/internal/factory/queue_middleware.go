package factory

import (
	"context"
	"errors"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkQueueMiddleware struct {
	QueueName   string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	ConsumerTag string
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
		QueueName:   queueName,
		Connection:  conn,
		Channel:     channel,
		ConsumerTag: "consumer-" + queueName,
	}, nil
}

func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	if q.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if q.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareMessage
	}
	msgs, err := q.Channel.Consume(
		q.QueueName,
		q.ConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := middleware.Message{Body: string(d.Body)}
		ack := func() {
			err := d.Ack(false)
			if err != nil {
				return
			}
		}
		nack := func() {
			err := d.Nack(false, true)
			if err != nil {
				return
			}
		}
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() error {
	if q.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Cancel(q.ConsumerTag, false); err != nil {
		if q.Channel.IsClosed() {
			return middleware.ErrMessageMiddlewareDisconnected
		}
		return middleware.ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *WorkQueueMiddleware) Send(msg middleware.Message) error {
	if q.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := q.Channel.PublishWithContext(ctx,
		DEFAULT_EXCHANGE,
		q.QueueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}
	return nil
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
