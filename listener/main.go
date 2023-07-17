package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func main() {
	topic := "my-topic" // Replace with your desired topic name

	listenToKafka(topic)
}

type MyData struct {
	Field1    string    `json:"field1"`
	Field2    int       `json:"field2"`
	Timestamp time.Time `json:"timestamp"`
}

func listenToKafka(topic string) {
	// Create a new Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Create a new Kafka partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}
	defer partitionConsumer.Close()

	// Handle OS signals to gracefully stop the consumer
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			// Handle the received message here
			sendElasticsearch(string(msg.Value))

		case <-signals:
			log.Println("Stopping the consumer...")
			return
		}
	}
}

type IndexResponse struct {
	ID string `json:"_id"`
}

func sendElasticsearch(message string) {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"}, // Replace with your Elasticsearch server address
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	data := MyData{
		Field1:    "Value1",
		Field2:    42,
		Timestamp: time.Now(),
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare the index request
	req := esapi.IndexRequest{
		Index:      "golang-test-index2", // Replace with your desired index name
		DocumentID: "",                   // Empty string to let Elasticsearch generate a unique ID
		Body:       strings.NewReader(string(jsonData)),
		Refresh:    "true",
	}

	// Send the index request to Elasticsearch
	res, err := req.Do(context.Background(), client)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Failed to index document: %s", res.String())
	}

	// Parse the response body to extract the document ID
	var indexRes IndexResponse
	if err := json.NewDecoder(res.Body).Decode(&indexRes); err != nil {
		log.Fatal(err)
	}

	// Print the response and generated document ID
	fmt.Println("Document indexed successfully!")
	fmt.Println("Response status:", res.Status())
	fmt.Println("Generated DocumentID:", indexRes.ID)
}
