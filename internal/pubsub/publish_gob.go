package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	ctx := context.Background()
	publishStruct := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	}
	err = ch.PublishWithContext(ctx, exchange, key, false, false, publishStruct)
	if err != nil {
		return err
	}
	return err
}
