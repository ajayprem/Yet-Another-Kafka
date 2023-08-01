package main

import (
	utils "Yet-Another-Kafka/Utils"
	"bufio"
	"bytes"
	"encoding/json"
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

// type CreateTopicCommand struct {
// 	fs         *flag.FlagSet
// 	topicName  string
// 	partitions int
// }

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">")
		scanner.Scan()
		err := scanner.Err()
		if err != nil {
			log.Fatal(err)
		}

		requestURL := fmt.Sprintf("http://localhost:%d/produce", 9988)

		var body utils.ProduceCommand
		body.TopicName = "bleh"
		body.Partitions = 0
		body.Message = scanner.Text()
		jsonBody, _ := json.Marshal(body)
		bodyReader := bytes.NewReader(jsonBody)

		req, err := http.NewRequest(http.MethodPost, requestURL, bodyReader)
		if err != nil {
			fmt.Printf("client: could not create request: %s\n", err)
			os.Exit(1)
		}

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("client: error making http request: %s\n", err)
			os.Exit(1)
		}

		fmt.Printf("client: got response!\n")
		fmt.Printf("client: status code: %d\n", res.StatusCode)
	}
}
