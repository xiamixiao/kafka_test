package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster" //support automatic consumer-group rebalancing and offset tracking
	"github.com/sdbaiguanghe/glog"
)

var (
	//	iplist = "10.100.159.117:9092"

	iplist = "172.18.32.72:9092,172.18.32.73:9092,172.18.32.74:9092"
)

func main() {
	groupID := "lmx04422-apm-cdn"
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	c, err := cluster.NewConsumer(strings.Split(iplist, ","), groupID, strings.Split("apm-cdn", ","), config)
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

	datamap := make(map[string]interface{})
	citymap := make(map[string]interface{})
	respmap := make(map[string]interface{})
	for msg := range c.Messages() {
		c.MarkOffset(msg, "") //MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
		json.Unmarshal(msg.Value, &datamap)
		response, _ := http.Get(fmt.Sprintf("http://mob.17usoft.com/ipService?req={\"ip\":%s,\"acc\":\"ad482ad5e7404f2082b98659ec10265d\",\"method\":\"getIpLoc\"}", datamap["remoteAddr"]))
		body, _ := ioutil.ReadAll(response.Body)
		json.Unmarshal(body, &citymap)
		//		respmap["version"] = datamap["version"]
		//		respmap["pageUrl"] = datamap["pageUrl"]
		//		respmap["url"] = datamap["url"]
		//		respmap["remoteAddr"] = datamap["remoteAddr"]
		respmap["provinceName"] = citymap["ProvinceName"]
		respmap["cityName"] = citymap["CityName"]
		respmap["time"] = datamap["time"]
		respmap["issuccess"] = datamap["issuccess"]
		respmap["deviceName"] = datamap["deviceName"]
		respmap["browserVersion"] = datamap["browserVersion"]
		respmap["browserName"] = datamap["browserName"]
		respmap["createTime"] = datamap["createTime"]
		r, _ := regexp.Compile("(\w+\.){2}\w+")
		s := r.FindStringSubmatch(datamap["url"].(string))
		fmt.Println(datamap["url"].(string), s)
		respmap["url"] = s[0]
		resp, _ := json.Marshal(respmap)
		producer(string(resp))
		//		fmt.Println(datamap["createTime"], citymap["CityName"])
	}
}

func producer(resp string) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	msg := &sarama.ProducerMessage{}
	msg.Topic = "apm-cdn-result"
	msg.Partition = int32(-1)
	msg.Key = sarama.StringEncoder("key")
	msg.Value = sarama.ByteEncoder(resp)
	producer, err := sarama.NewSyncProducer(strings.Split(iplist, ","), config)
	if err != nil {
		fmt.Println("1:", err)
		os.Exit(500)
	}
	defer producer.Close()
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		fmt.Println("2:", err)
		os.Exit(500)
	}
}
