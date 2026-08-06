package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Fatalf("Error declaring and binding queue: %v", err)
	}
	fmt.Printf("Queue %v created\n", q.Name)

	deliveries, err := channel.Consume("", "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error consuming channel: %v", err)
	}

	go func() {
		for delivery := range deliveries {
			var msg T
			err := json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				log.Fatalf("Error unmarshalling data: %v", err)
			}
			msgResult := handler(msg)
			switch msgResult {
			case 0:
				err = delivery.Ack(false)
				if err != nil {
					log.Fatalf("Error acknowledging message: %v", err)
				}
				fmt.Println("Message acknowledged")
			case 1:
				err = delivery.Nack(false, true)
				if err != nil {
					log.Fatalf("Error acknowledging message (nack requeue): %v", err)
				}
				fmt.Println("Message not acknowledged, requeued")
			case 2:
				err = delivery.Nack(false, false)
				if err != nil {
					log.Fatalf("Error acknowledging message (nack discard): %v", err)
				}
				fmt.Println("Message not acknowledged, discarded")
			}
			/* err = delivery.Ack(false)
			if err != nil {
				log.Fatalf("Error acknowledging message: %v", err)
			} */
		}
	}()

	return err
}
