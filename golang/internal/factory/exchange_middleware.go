package factory

import (
	"context"
	"errors"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	exchange    string
	bindings    []string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	QueueName   string
	ConsumerTag string
}

func NewExchangeMiddleware(exchange string, keys []string, conn *amqp.Connection, channel *amqp.Channel) (*ExchangeMiddleware, error) {
	err := channel.ExchangeDeclare(exchange, DIRECT_EXCHANGE, false, false, false, false, nil)
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

	queue, err := channel.QueueDeclare(
		DEFAULT_QUEUE_NAME,
		true,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}

	for _, key := range keys {
		err = channel.QueueBind(
			queue.Name,
			key,
			exchange,
			false,
			nil)
		if err != nil {
			closeErr := closeResources(conn, channel)
			return nil, errors.Join(err, closeErr)
		}
	}
	return &ExchangeMiddleware{
		exchange:    exchange,
		bindings:    keys,
		Connection:  conn,
		Channel:     channel,
		QueueName:   queue.Name,
		ConsumerTag: "consumer-" + exchange,
	}, nil
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	if e.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if e.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareMessage
	}
	msgs, err := e.Channel.Consume(
		e.QueueName,
		e.ConsumerTag,
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

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if err := e.Channel.Cancel(e.ConsumerTag, false); err != nil {
		if e.Channel.IsClosed() {
			return middleware.ErrMessageMiddlewareDisconnected
		}
		return middleware.ErrMessageMiddlewareMessage
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg middleware.Message) error {
	if e.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range e.bindings {
		err := e.Channel.PublishWithContext(ctx,
			e.exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return middleware.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	err := e.Channel.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	err = e.Connection.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
