package middleware

import (
	"context"
	"errors"
	"time"

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
		connErr := conn.Close()
		chErr := channel.Close()
		err = errors.Join(err, connErr, chErr)
		return nil, err
	}

	err = channel.Qos(
		PREFETCH_COUNT,
		PREFETCH_SIZE,
		GLOBAL,
	)
	if err != nil {
		connErr := conn.Close()
		chErr := channel.Close()
		err = errors.Join(err, connErr, chErr)
		return nil, err
	}
	return &WorkQueueMiddleware{
		QueueName:   queueName,
		Connection:  conn,
		Channel:     channel,
		ConsumerTag: "consumer-" + queueName,
	}, nil
}

func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
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
		return ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := Message{Body: string(d.Body)}
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
		return ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Cancel(q.ConsumerTag, false); err != nil {
		if q.Channel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *WorkQueueMiddleware) Send(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

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
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *WorkQueueMiddleware) Close() error {
	err := q.Channel.Close()
	if err != nil {
		return ErrMessageMiddlewareClose
	}
	err = q.Connection.Close()
	if err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
