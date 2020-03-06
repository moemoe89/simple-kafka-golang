//
//  Practicing Kafka
//
//  Copyright © 2016. All rights reserved.
//

package main

import (
	conf "github.com/moemoe89/simple-kafka-golang/producer/config"

	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/Shopify/sarama"
)

type Request struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var request Request

const (
	KAFKA_TOPIC = "simple-kafka-golang"
)

func message(c *gin.Context) {
	c.Bind(&request)
	reqMarshal,err := json.Marshal(request)
	if err != nil {
		panic(err)
	}

	reqString := string(reqMarshal)

	producer, err := conf.InitKafkaProducer()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	strTime := strconv.Itoa(int(time.Now().Unix()))

	msg := &sarama.ProducerMessage{
		Topic: KAFKA_TOPIC,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder(reqString),
	}

	producer.Input() <- msg

	resp := gin.H{
		"status":  http.StatusOK,
		"message": "Message has been sent.",
		"data":    reqString,
	}

	c.IndentedJSON(http.StatusOK, resp)
}

func main() {
	router := gin.Default()
	router.POST("/",message)
	router.Run(":"+conf.Configuration.Port)
}