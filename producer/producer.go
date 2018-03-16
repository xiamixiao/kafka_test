package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/Shopify/sarama"
	"github.com/sdbaiguanghe/glog"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8088", nil))
	}()

	for {
		producer()
	}
}

func producer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true //必须有这个选项
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewAsyncProducer(strings.Split("10.100.159.117:9092", ","), config)
	defer p.Close()
	if err != nil {
		return
	}

	//必须有这个匿名函数内容
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					glog.Errorln(err)
				}
			case <-success:
			}
		}
	}(p)

	v := "测试" + time.Now().Format("15:04:05")
	fmt.Fprintln(os.Stdout, v)
	msg := &sarama.ProducerMessage{
		Topic: "apm-cdn",
		Value: sarama.ByteEncoder(v),
	}
	p.Input() <- msg
}
