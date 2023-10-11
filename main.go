
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Person struct {
	Name string `json:"name"`
	Surname string `json:"surname"`
	Patronymic string `json:"patronymic,omitempty"`
}

type EnrichedPerson struct {
	Person Person `json:"person"`
	Age int `json:"age"`
	Gender string `json:"gender"`
	Nationality string `json:"nationality"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func main() {
	// Kafka consumer configuration
	consumerConfig := &kafka.ConfigMap{
	"bootstrap.servers": "localhost:9092",
	"group.id":          "enrichment-service",
	"auto.offset.reset": "earliest",
 }

	// Create Kafka consumer instance
	consumer, err := kafka.NewConsumer(consumerConfig)
		if err != nil {
			log.Fatalf("Failed to create Kafka consumer: %s", err)
		}
 	defer consumer.Close()

	// Subscribe to FIO topic
	err = consumer.SubscribeTopics([]string{"FIO"}, nil)
		if err != nil {
			log.Fatalf("Failed to subscribe to Kafka topic: %s", err)
		}

	// Kafka producer configuration
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	// Create Kafka producer instance
	producer, err := kafka.NewProducer(producerConfig)
		if err != nil {
			log.Fatalf("Failed to create Kafka producer: %s", err)
		}
	defer producer.Close()

 for {
	// Poll Kafka messages
	msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Failed to read Kafka message: %s", err)
			continue
		}

	// Parse person data from JSON message
	var person Person
	err = json.Unmarshal(msg.Value, &person)
		if err != nil {
			log.Printf("Failed to parse Kafka message: %s", err)
			// Send failed message to Kafka
			failedMessage := EnrichedPerson{
				Person:        person,
				ErrorMessage:  "Failed to parse message: " + err.Error(),
			}
			sendFailedMessage(producer, failedMessage)
			continue
		}

	// Enrich person data
	enrichedPerson, err := enrichPerson(person)
		if err != nil {
		log.Printf("Failed to enrich person data: %s", err)
		// Send failed message to Kafka
			failedMessage := EnrichedPerson {
				Person:        person,
				ErrorMessage:  "Failed to enrich person data: " + err.Error(),
			}
		sendFailedMessage(producer, failedMessage)
		continue
		}

	// Save enriched person data to database
	err = saveEnrichedPerson(enrichedPerson)
		if err != nil {
			log.Printf("Failed to save enriched person data: %s", err)
			// Send failed message to Kafka
			failedMessage := EnrichedPerson{
				Person:        person,
				ErrorMessage:  "Failed to save enriched person data: " + err.Error(),
			}
			sendFailedMessage(producer, failedMessage)
			continue
		}

	// Send enriched person data to Kafka
	sendEnrichedPerson(producer, enrichedPerson)
 	}
}

func enrichPerson(person Person) (EnrichedPerson, error) {
	// TODO: Implement person data enrichment logic
	return EnrichedPerson{
		Person:      person,
		Age:         30,
		Gender:      "male",
		Nationality: "Russian",
	}, nil
}

func saveEnrichedPerson(enrichedPerson EnrichedPerson) error {
	// TODO: Implement enriched person data saving logic
	fmt.Printf("Saved enriched person data: %+v\n", enrichedPerson)
	return nil
}

func sendEnrichedPerson(producer *kafka.Producer, enrichedPerson EnrichedPerson) {
	enrichedPersonJSON, err := json.Marshal(enrichedPerson)
	if err != nil {
		log.Printf("Failed to marshal enriched person data: %s", err)
		return
 	}

	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &[]string{"ENRICHED_FIO"}[0],
			Partition: kafka.PartitionAny,
		},
		Value: enrichedPersonJSON,
	}, deliveryChan)

	if err != nil {
		log.Printf("Failed to send enriched person data to Kafka: %s", err)
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Failed to deliver enriched person data to Kafka: %s", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered enriched person data to Kafka: %v\n", enrichedPerson)
 	}	
}

func sendFailedMessage(producer *kafka.Producer, failedMessage EnrichedPerson) {
	failedMessageJSON, err := json.Marshal(failedMessage)
	if err != nil {
		log.Printf("Failed to marshal failed message data: %s", err)
		return
	}

	deliveryChan := make(chan kafka.Event, 1)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &[]string{"FIO_FAILED"}[0],
			Partition: kafka.PartitionAny,
		},
		Value: failedMessageJSON,
	}, deliveryChan)

	if err != nil {
		log.Printf("Failed to send failed message to Kafka: %s", err)
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Failed to deliver failed message to Kafka: %s", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered failed message to Kafka: %v\n", failedMessage)
	}
}