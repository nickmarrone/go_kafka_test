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
var op string

func init() {
	flag.Var(&brokers, "brokers", "comma separated list of brokers")
	flag.StringVar(&op, "op", "a", "Mathmateical operation to use. + => a, - => s, * => m, / => d")
	flag.StringVar(&topic, "topic", "multiply", "Kafka topic to read messages from (default 'multiply')")
}

// Receive 'Multiply' messages and process them
type Multiply struct {
	X, Y int
}

type ProcessFunc func(Multiply)

func Serve(queue <-chan *sarama.ConsumerMessage, pf ProcessFunc) {
	var sem = make(chan struct{}, 1)

	for rawMsg := range queue {
		sem <- struct{}{}
		go func(rawMsg *sarama.ConsumerMessage) {
			var msg Multiply
			err := json.Unmarshal(rawMsg.Value, &msg)
			if err != nil {
				log.Fatalln(err)
			}

			pf(msg)
			<-sem
		}(rawMsg)
	}
}

func processAdd(msg Multiply) {
	fmt.Printf("%d + %d = %d\n", msg.X, msg.Y, msg.X+msg.Y)
}

func processSub(msg Multiply) {
	fmt.Printf("%d - %d = %d\n", msg.X, msg.Y, msg.X-msg.Y)
}

func processMul(msg Multiply) {
	fmt.Printf("%d * %d = %d\n", msg.X, msg.Y, msg.X*msg.Y)
}

func processDiv(msg Multiply) {
	fmt.Printf("%d / %d = %f\n", msg.X, msg.Y, float64(msg.X)/float64(msg.Y))
}

func main() {
	flag.Parse()

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	var pf ProcessFunc
	switch {
	case "+" == op:
		pf = processAdd
	case "-" == op:
		pf = processSub
	case "*" == op:
		pf = processMul
	case "/" == op:
		pf = processDiv
	}

	// Set up one partition_consumer for each partition
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln(err)
	}

	partition_consumers := make([]sarama.PartitionConsumer, len(partitions))
	for idx, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalln(err)
		}

		partition_consumers[idx] = pc
		go func(pc sarama.PartitionConsumer) {
			Serve(pc.Messages(), pf)
		}(pc)

		go func(pc sarama.PartitionConsumer) {
			for err := range pc.Errors() {
				log.Println(err)
			}
		}(pc)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	for _, pc := range partition_consumers {
		fmt.Println("Closing partition, next offset", pc.HighWaterMarkOffset())
		pc.AsyncClose()
	}
}
