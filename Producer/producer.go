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
)

var (
	zookeeperURL = fmt.Sprintf("http://localhost:%d/leader", 9998)
)

func main() {
	var TopicName string
	var Partition int
	flag.StringVar(&TopicName, "topic", "default", "Name of the topic to be created")
	flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.Parse()
	fmt.Println(TopicName)

	// Connect to zookeeper to find the leader broker
	res, err := http.Get(zookeeperURL)
	if err != nil {
		log.Fatalf("Producer: Unable to connect to Zookeeper to find the leader: %s\n", err)
	}

	var port int
	json.NewDecoder(res.Body).Decode(&port)
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
			os.Exit(1)
		}

		_, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Producer: error making http request: %s\n", err)
			os.Exit(1)
		}
	}
}
