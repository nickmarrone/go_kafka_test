package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

// Set up flags
type Brokers []string

func (b *Brokers) String() string {
	return fmt.Sprint(*b)
}

func (b *Brokers) Set(value string) error {
	for _, broker := range strings.Split(value, ",") {
		*b = append(*b, broker)
	}
	return nil
}

var brokers Brokers
var topic string

func init() {
	flag.Var(&brokers, "brokers", "comma separated list of brokers")
	flag.StringVar(&topic, "topic", "multiply", "Kafka topic to send messages to (default 'multiply')")
}

// Create 'Multiply' messages and send them
type Multiply struct {
	X, Y int
}

func Serve(producer sarama.SyncProducer, topic string) {
	for {
		fmt.Print("x y: ")
		var x, y int
		fmt.Scanf("%d %d", &x, &y)

		m := Multiply{
			X: x,
			Y: y,
		}

		jsonMsg, err := json.Marshal(m)
		if err != nil {
			log.Fatalln(err)
		}

		msg := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(jsonMsg),
		}

		partition, offset, err := producer.SendMessage(&msg)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println("Sent msg to partition:", partition, ", offset:", offset)
		}
	}
}

func main() {
	flag.Parse()

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go Serve(producer, topic)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	fmt.Println("Shutting down data collector.")
	if err := producer.Close(); err != nil {
		log.Println("Failed to close data collector correctly:", err)
	}
}
