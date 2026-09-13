package factory

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

func closeResources(conn *amqp.Connection, channel *amqp.Channel) error {
	connErr := conn.Close()
	chErr := channel.Close()
	return errors.Join(connErr, chErr)
}
