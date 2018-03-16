package main

import (
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	for {
		producer()
	}
}

func producer() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	msg := &sarama.ProducerMessage{}
	msg.Topic = "apm-cdn"
	msg.Partition = int32(-1)
	msg.Key = sarama.StringEncoder("key")
	msg.Value = sarama.ByteEncoder("你好, 世界!" + time.Now().Format("15:04:05"))
	producer, err := sarama.NewSyncProducer(strings.Split("10.100.159.117:9092", ","), config)
	if err != nil {
		os.Exit(500)
	}
	defer producer.Close()
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		os.Exit(500)
	}
}
