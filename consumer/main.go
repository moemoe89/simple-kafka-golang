//
//  Practicing Kafka
//
//  Copyright © 2016. All rights reserved.
//

package main

import (
	conf "github.com/moemoe89/simple-kafka-golang/consumer/config"

	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

const (
	KAFKA_TOPIC = "simple-kafka-golang"
)

func main(){
	consumer, err := conf.InitKafkaConsumer()
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