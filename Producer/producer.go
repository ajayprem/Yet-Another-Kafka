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

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var (
	URL = fmt.Sprintf("http://localhost:%d/produce", 9988)
)

func main() {
	var TopicName string
	var Partition int
	flag.StringVar(&TopicName, "topic", "default", "Name of the topic to be created")
	flag.IntVar(&Partition, "partition", 0, "Partition to write to")
	flag.Parse()
	fmt.Println(TopicName)

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

		req, err := http.NewRequest(http.MethodPost, URL, bodyReader)
		if err != nil {
			fmt.Printf("client: could not create request: %s\n", err)
			os.Exit(1)
		}

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("client: error making http request: %s\n", err)
			os.Exit(1)
		}
		fmt.Printf("client: status code: %d\n", res.StatusCode)
	}
}
