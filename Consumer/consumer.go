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

	"github.com/gorilla/mux"
)

var (
	URL = fmt.Sprintf("http://localhost:%d/register", 9988)
)

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

	// Register to broker
	var body utils.RegisterCommand
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
