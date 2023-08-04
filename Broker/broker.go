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
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
)

const (
	PARTITIONS    = 0
	LOGS_LOCATION = "/tmp"
)

var (
	BROKER_ID    int
	zookeeperURL = fmt.Sprintf("http://localhost:%d/register", 9998)
	isLeader     = false
)

func createTopic(topicName string, partitions int) {
	topicDir := filepath.Join(LOGS_LOCATION, topicName+strconv.Itoa(partitions))

	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		if err := os.Mkdir(topicDir, os.ModePerm); err != nil {
			log.Fatalf("Unable to create topic folder: %s", err)
		}
		file, err := os.Create(filepath.Join(topicDir, "log.txt"))
		if err != nil {
			log.Fatalf("Failed creating file: %s", err)
		}
		file.Close()
	}
}

func ProduceHandler(w http.ResponseWriter, r *http.Request) {
	var command utils.ProduceMessage
	json.NewDecoder(r.Body).Decode(&command)

	createTopic(command.TopicName, 0)
	topicDir := filepath.Join(LOGS_LOCATION, command.TopicName+strconv.Itoa(command.Partitions))
	file, err := os.OpenFile(filepath.Join(topicDir, "log.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed opening file: %s", err)
	}
	defer file.Close()

	datawriter := bufio.NewWriter(file)
	_, err = datawriter.WriteString(command.Message + "\n")
	if err != nil {
		log.Fatalf("Unable to write to file: %s", err)
	}
	datawriter.Flush()

	w.Header().Set("Content-Type", "application/json")
}

func RegisterConsumer(w http.ResponseWriter, r *http.Request) {
	var command utils.RegisterConsumer
	json.NewDecoder(r.Body).Decode(&command)

	createTopic(command.TopicName, 0)
	topicDir := filepath.Join("tmp", command.TopicName+"0")
	fmt.Println(topicDir)
	file, _ := os.Open(filepath.Join(topicDir, "log.txt"))
	defer file.Close()

	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	var messages []string

	for fileScanner.Scan() {
		messages = append(messages, fileScanner.Text())
	}
	fmt.Println(messages)

	jsonResponse, jsonError := json.Marshal(messages)
	if jsonError != nil {
		fmt.Println("Unable to encode JSON")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func LeaderHandler(w http.ResponseWriter, r *http.Request) {
	isLeader = true
	w.WriteHeader(200)
}

func main() {

	var Port int
	flag.IntVar(&Port, "port", 9988, "Port for broker to run")
	flag.Parse()


	// Register with zookeeper
	log.Println("Broker: Registering with zookeeper:")
	
	var body utils.RegisterBroker
	body.Port = Port

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, _ := http.NewRequest(http.MethodPost, zookeeperURL, bodyReader)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Broker: Error connecting with Zookeeper: %s\n", err)
	}
	if res.StatusCode != 200 {
		log.Fatalf("Broker: Unable to register with Zookeeper:\n")
	}

	json.NewDecoder(res.Body).Decode(&BROKER_ID)

	log.Println("Starting Broker id:", BROKER_ID)

	// Listen for producers or consumers
	r := mux.NewRouter()
	r.HandleFunc("/produce", ProduceHandler).Methods("POST")
	r.HandleFunc("/register", RegisterConsumer).Methods("POST")
	r.HandleFunc("/health", HealthHandler)
	r.HandleFunc("/leader", LeaderHandler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(Port), r))
}
