package main

import (
	utils "Yet-Another-Kafka/Utils"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
)

var (
	URL = fmt.Sprintf("http://localhost:%d/consume", 9988)
)

func main() {
	var TopicName string
	var Partition int
	flag.StringVar(&TopicName, "topic", "default", "Name of the topic to be created")
	flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.Parse()

	// for {
	var body utils.ConsumeCommand
	body.TopicName = TopicName
	body.Partitions = 0

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest(http.MethodPost, URL, bodyReader)
	if err != nil {
		fmt.Printf("client: could not create request: %s\n", err)
		os.Exit(1)
	}

	res, err := http.DefaultClient.Do(req)

	var messages []string
	json.NewDecoder(res.Body).Decode(&messages)
	fmt.Println(messages)
	if err != nil {
		fmt.Printf("client: error making http request: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("client: status code: %d\n", res.StatusCode)
	// }
}
