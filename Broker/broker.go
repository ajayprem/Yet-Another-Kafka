// socket-server project main.go
package main

import (
	utils "Yet-Another-Kafka/Utils"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
)

const (
	BROKER_ID     = 0
	PARTITIONS    = 0
	LOGS_LOCATION = "/tmp"
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
	var command utils.ProduceCommand
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

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	var command utils.ConsumeCommand
	json.NewDecoder(r.Body).Decode(&command)

	createTopic(command.TopicName, 0)
	topicDir := filepath.Join("tmp", command.TopicName+"0")
	file, _ := os.Open(filepath.Join(topicDir, "log.txt"))
	defer file.Close()

	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	var messages []string

	for fileScanner.Scan() {
		messages = append(messages, fileScanner.Text())
	}

	jsonResponse, jsonError := json.Marshal(messages)
	if jsonError != nil {
		fmt.Println("Unable to encode JSON")
	}
	fmt.Println(jsonResponse)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)

}

func main() {
	fmt.Println("Starting Broker id:", BROKER_ID)
	r := mux.NewRouter()
	r.HandleFunc("/produce", ProduceHandler).Methods("POST")
	r.HandleFunc("/consume", ConsumeHandler)

	log.Fatal(http.ListenAndServe(":9988", r))
}
