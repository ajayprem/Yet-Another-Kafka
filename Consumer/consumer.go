package main

import (
	utils "Yet-Another-Kafka/Utils"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

var (
	zookeeperURL = fmt.Sprintf("http://localhost:%d/broker", 9998)
)

func connectToBroker() int {
	// Connect to zookeeper to find the leader broker
	count := 0
	for count < 5 {
		res, err := http.Get(zookeeperURL)
		if err != nil {
			log.Fatalf("Consumer: Unable to connect to Zookeeper to find the leader: %s\n", err)
		}

		var body utils.BrokerResponse
		json.NewDecoder(res.Body).Decode(&body)
		if body.Id != -1 {
			return body.Port
		}
		log.Println("Consumer: Unable to connect to leader broker: Retrying")
		time.Sleep(time.Second * 5)
		count += 1
	}
	log.Fatalf("Producer: Leader broker unavailable")
	return 0
}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
}

func main() {
	var TopicName string
	var Partition int
	var Port int
	flag.StringVar(&TopicName, "topic", "default", "Name of the topic to be created")
	flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.IntVar(&Port, "port", 9999, "Port to run Consumer on")
	flag.Parse()

	// Connect to a broker from the cluster
	brokerPort := connectToBroker()
	URL := fmt.Sprintf("http://localhost:%d/register", brokerPort)

	// Register to broker
	var body utils.RegisterConsumer
	body.TopicName = TopicName
	body.Partitions = 0
	body.Port = Port

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, _ := http.NewRequest(http.MethodPost, URL, bodyReader)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Consumer: Error connecting with Broker: %s\n", err)
	}

	var messages []string
	json.NewDecoder(res.Body).Decode(&messages)

	for _, message := range messages {
		fmt.Println(">", message)
	}

	//create server to wait for messages from broker
	r := mux.NewRouter()
	r.HandleFunc("/consume", ConsumeHandler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(Port), r))
}
