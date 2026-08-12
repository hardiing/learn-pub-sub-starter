package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionStr := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Error making connection: %v", err)
	}
	defer connection.Close()

	newChannel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error making new channel in connection: %v", err)
	}

	fmt.Println("Connection to server was successful.")

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.SimpleQueueDurable, handlerLogs())
	if err != nil {
		log.Fatalf("Error consuming logs: %v", err)
	}

	gamelogic.PrintServerHelp()
Loop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a command")
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Error publishing JSON: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Error publishing JSON: %v", err)
			}
		case "quit":
			fmt.Println("Quitting server connection...")
			break Loop
		default:
			fmt.Println("Command not found")
		}
	}

	//signalChan := make(chan os.Signal, 1)
	//signal.Notify(signalChan, os.Interrupt)
	//<-signalChan
	fmt.Println("\nShutting down program.")
}
