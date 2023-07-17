package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	topic := "my-topic"                  // Replace with your desired topic name
	message := "Hello, Kafka to Golang!" // Replace with your message payload

	sendMessageToKafka(topic, message)

}

func sendMessageToKafka(topic string, message string) {
	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// Create a new Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Create a ticker for every 5 seconds
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send the message to Kafka
			_, _, err := producer.SendMessage(msg)
			if err != nil {
				log.Println("Failed to send message:", err)
			} else {
				log.Println("Message sent to Kafka")
			}
		}
	}
}
