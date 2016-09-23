package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/Shopify/sarama"
	"net/http"
	"strconv"
	"time"
)

type Request struct {
	ID int `json:"id"`
	Name  string `json:"name"`
}

var request Request

const (
	PRODUCER_URL string = "localhost:9092"
	KAFKA_TOPIC string = "simple-kafka-golang"
)

func message(c *gin.Context) {

	c.Bind(&request)
	reqMarshal,err := json.Marshal(request)

	if err != nil {
		panic(err)
	}

	reqString := string(reqMarshal)

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{PRODUCER_URL}
	producer, err := sarama.NewAsyncProducer(brokers, config)
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
		"status": http.StatusOK,
		"message": "Message has been sent.",
		"data": reqString,
	}

	c.IndentedJSON(http.StatusOK, resp)

}

func main() {

	router := gin.Default()
	router.POST("/",message)
	router.Run(":3000")

}