package main

import (
	utils "Yet-Another-Kafka/Utils"
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	zookeeperURL = fmt.Sprintf("http://localhost:%d/leader", 9998)
)

func connectToBroker() int {
	// Connect to zookeeper to find the leader broker
	var port int
	count := 0
	fmt.Println("here")
	for count < 5 {
		res, err := http.Get(zookeeperURL)
		if err != nil {
			log.Fatalf("Producer: Unable to connect to Zookeeper to find the leader: %s\n", err)
		}

		json.NewDecoder(res.Body).Decode(&port)
		if port != -1 {
			return port
		}
		log.Println("Producer: Unable to connect to leader broker: Retrying")
		time.Sleep(time.Second * 5)
		count += 1
	}
	log.Fatalf("Producer: Leader broker unavailable")
	return 0
}

func main() {
	var TopicName string
	var Partition int
	flag.StringVar(&TopicName, "topic", "default", "Name of the topic to be created")
	flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.Parse()
	fmt.Println(TopicName)

	port := connectToBroker()
	leader_url := fmt.Sprintf("http://localhost:%d/produce", port)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		err := scanner.Err()
		if err != nil {
			log.Fatal(err)
		}

		var body utils.ProduceMessage
		body.TopicName = TopicName
		body.Partitions = 0
		body.Message = scanner.Text()
		jsonBody, _ := json.Marshal(body)
		bodyReader := bytes.NewReader(jsonBody)

		req, err := http.NewRequest(http.MethodPost, leader_url, bodyReader)
		if err != nil {
			log.Printf("Producer: could not create request: %s\n", err)
			port = connectToBroker()
			leader_url = fmt.Sprintf("http://localhost:%d/produce", port)
		}

		_, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Producer: Error connecting with broker, please try again: %s\n", err)
			port = connectToBroker()
			leader_url = fmt.Sprintf("http://localhost:%d/produce", port)
		}
	}
}
