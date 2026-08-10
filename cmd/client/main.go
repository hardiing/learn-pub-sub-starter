package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionStr := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Error making connection: %v", err)
	}
	defer connection.Close()

	fmt.Println("Connection to server was successful.")

	clientName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error getting client welcome message: %v", err)
	}

	newGame := gamelogic.NewGameState(clientName)

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+clientName,
		routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(newGame))
	if err != nil {
		log.Fatalf("Error subscribing to pause messages: %v", err)
	}

	newChannel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error making new channel in connection: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+clientName,
		routing.ArmyMovesPrefix+".*", pubsub.SimpleQueueTransient, handlerMove(newGame, newChannel))
	if err != nil {
		log.Fatalf("Error subscribing to move messages: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable, handlerWar(newGame, newChannel))
	if err != nil {
		log.Fatalf("Error subscribing to war messages: %v", err)
	}

Loop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a command")
		}
		switch input[0] {
		case "spawn":
			err = newGame.CommandSpawn(input)
			if err != nil {
				fmt.Println("Invalid spawn command")
			}
		case "move":
			mv, err := newGame.CommandMove(input)
			if err != nil {
				fmt.Println("Invalid move command")
				break
			}
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+clientName, mv)
			if err != nil {
				fmt.Printf("Error publishing move: %v", err)
				break
			}
			fmt.Println("Move successful")
		case "status":
			newGame.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
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

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}
