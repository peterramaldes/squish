package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TODO: Write the consumers
// TODO: Implemenation options to guarantee idempotency on the consumers

func main() {
	// Connect to RabbitMQ Server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create a Channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a Queue
	queue, err := ch.QueueDeclare(
		"hello", // Name
		false,   // Durable
		false,   // Delete when unused
		false,   // Exclusive
		false,   // No wait
		nil,     // Arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello World!"
	err = ch.PublishWithContext(ctx,
		"",         // Exchange
		queue.Name, // Routing Key
		false,      // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	failOnError(err, "Failed to publish a message")

	log.Printf("\t[x] Sent %s\n", body)
}

// Helper Functions (Could be moved to another file)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
