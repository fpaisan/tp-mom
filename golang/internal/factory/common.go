package factory

import (
	"errors"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func closeResources(conn *amqp.Connection, channel *amqp.Channel) error {
	connErr := conn.Close()
	chErr := channel.Close()
	return errors.Join(connErr, chErr)
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

func consumeMessages(queueName string, channel *amqp.Channel, consumerTag string, callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := channel.Consume(
		queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}
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

func cancelChannel(queueName string, channel *amqp.Channel) error {
	if err := channel.Cancel(queueName, false); err != nil {
		if channel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func publish(channel *amqp.Channel, exchange string, routingKey, body string) error {
	err := channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}
