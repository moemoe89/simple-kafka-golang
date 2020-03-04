package main

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

const (
	PRODUCER_URL = "localhost:9092"
	KAFKA_TOPIC  = "simple-kafka-golang"
)

func main(){

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Consumer.Return.Errors = true

	producerUrl := strings.Split(PRODUCER_URL, ",")

	consumer, err := sarama.NewConsumer(producerUrl, config)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition(KAFKA_TOPIC, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	log.Print("Connected to kafka broker")

	for m := range partitionConsumer.Messages() {

		text := string(m.Value)
		bytes := []byte(text)

		var user User
		json.Unmarshal(bytes, &user)

		log.Print("raw : ",text)
		log.Print("user id : ",user.ID)
		log.Print("name : ",user.Name)

	}

}