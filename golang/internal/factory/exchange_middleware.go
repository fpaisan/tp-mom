package factory

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	exchange    string
	bindings    []string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	QueueName   string
	isConsuming bool
}

func NewExchangeMiddleware(exchange string, keys []string, conn *amqp.Connection, channel *amqp.Channel) (*ExchangeMiddleware, error) {
	err := channel.ExchangeDeclare(exchange, DIRECT_EXCHANGE, true, false, false, false, nil)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}

	return &ExchangeMiddleware{
		exchange:   exchange,
		bindings:   keys,
		Connection: conn,
		Channel:    channel,
	}, nil
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	err := checkResources(e.isConsuming, e.Connection, e.Channel)
	if err != nil {
		return err
	}
	queueName, err := queueDeclare(e.Channel, DEFAULT_QUEUE_NAME, EXCHANGE_QUEUE_DURABILITY, EXCHANGE_QUEUE_EXCLUSIVITY)
	if err != nil {
		if e.Channel.IsClosed() {
			return middleware.ErrMessageMiddlewareDisconnected
		}
		return middleware.ErrMessageMiddlewareMessage
	}
	e.QueueName = queueName
	for _, key := range e.bindings {
		err = e.Channel.QueueBind(
			queueName,
			key,
			e.exchange,
			false,
			nil)
		if err != nil {
			if e.Channel.IsClosed() {
				return middleware.ErrMessageMiddlewareDisconnected
			}
			return middleware.ErrMessageMiddlewareMessage
		}
	}
	e.isConsuming = true
	if err := consumeMessages(e.QueueName, e.Channel, e.QueueName, callbackFunc); err != nil {
		e.isConsuming = false
		return err
	}
	if e.isConsuming {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if !e.isConsuming {
		return nil
	}
	e.isConsuming = false
	return cancelChannel(e.QueueName, e.Channel)
}

func (e *ExchangeMiddleware) Send(msg middleware.Message) error {
	if e.Channel.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	if e.Connection.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.bindings {
		if err := publish(e.Channel, e.exchange, key, msg.Body); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if err := closeResources(e.Connection, e.Channel); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
