package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster" //support automatic consumer-group rebalancing and offset tracking
	"github.com/sdbaiguanghe/glog"
)

var (
	iplist = "172.18.32.72:9092,172.18.32.73:9092,172.18.32.74:9092"

//	iplist = "10.100.159.117:9092"
)

// consumer 消费者
func main() {
	groupID := "lmx04422-apm-cdn-result"
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //初始从最新的offset开始

	c, err := cluster.NewConsumer(strings.Split(iplist, ","), groupID, strings.Split("apm-fronted", ","), config)
	if err != nil {
		glog.Errorf("Failed open consumer: %v", err)
		return
	}
	defer c.Close()
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				glog.Errorln(err)
			case <-noti:
			}
		}
	}(c)

	for msg := range c.Messages() {
		fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		fmt.Println("----------------------")
	}
}
