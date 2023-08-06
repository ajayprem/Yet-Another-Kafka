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

const (
	MAX_FAIL_RETRY = 5
)

var (
	zookeeperURL = fmt.Sprintf("http://localhost:%d/broker", 9998)
	brokerId     = -1
	brokerPort   = -1
	URL          string
	Port         int
	topicName    string
)

func registerToBroker() {
	// Inform the broker about the consumer's location so that the broker can send messages from the topic
	var body utils.RegisterConsumer
	body.TopicName = topicName
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
}

func connectToBroker() {
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
			brokerId = body.Id
			brokerPort = body.Port
			URL = fmt.Sprintf("http://localhost:%d/register", brokerPort)
			log.Println("Consumer: Connected to broker: ", brokerId)
			registerToBroker()
			return
		}
		log.Println("Consumer: Unable to connect to leader broker: Retrying")
		time.Sleep(time.Second * 5)
		count += 1
	}
	log.Fatalf("Consumer: No Brokers Available")
}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
}

func checkBroker() {
	for {
		if brokerId != -1 {
			url := fmt.Sprintf("http://localhost:%d/health?id=%d", brokerPort, brokerId)

			res, err := http.Get(url)
			if err != nil || res.StatusCode != 200 {
				log.Println("Consumer: Unable to connect to Broker: Fetching new broker id from zookeeper")
				connectToBroker()
			}
		}
		time.Sleep(time.Second * 10)
	}
}

func main() {

	flag.StringVar(&topicName, "topic", "default", "Name of the topic to be created")
	// flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.IntVar(&Port, "port", 9997, "Port to run Consumer on")
	// flag.BoolVar(&Port, "port", 9997, "Port to run Consumer on")

	flag.Parse()

	// Connect to a broker from the cluster
	connectToBroker()
	go checkBroker()

	//create server to wait for messages from broker
	r := mux.NewRouter()
	r.HandleFunc("/consume", ConsumeHandler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(Port), r))
}
