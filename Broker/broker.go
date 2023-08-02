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

	"github.com/gorilla/mux"
)

const (
	BROKER_ID     = 0
	PARTITIONS    = 0
	LOGS_LOCATION = "/tmp"
)

func createTopic(topicName string, partitions int) *os.File {
	topicDir := filepath.Join("tmp", topicName+"0")

	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		if err := os.Mkdir(topicDir, os.ModePerm); err != nil {
			log.Fatal(err)
		}
		file, err := os.Create(filepath.Join(topicDir, "log.txt"))
		if err != nil {
			log.Fatalf("Failed creating file: %s", err)
		}
		return file
	}
	file, err := os.OpenFile(filepath.Join(topicDir, "log.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed opening file: %s", err)
	}
	return file
}

func ProduceHandler(w http.ResponseWriter, r *http.Request) {
	var command utils.ProduceCommand
	json.NewDecoder(r.Body).Decode(&command)

	file := createTopic(command.TopicName, 0)
	defer file.Close()

	datawriter := bufio.NewWriter(file)
	_, err := datawriter.WriteString(command.Message + "\n")
	if err != nil {
		log.Fatalf("Unable to write to file: %s", err)
	}
	datawriter.Flush()

	w.Header().Set("Content-Type", "application/json")
}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	var command utils.ConsumeCommand
	json.NewDecoder(r.Body).Decode(&command)

	file := createTopic(command.TopicName, 0)
	defer file.Close()

}

func main() {
	fmt.Println("Starting Broker id:", BROKER_ID)
	r := mux.NewRouter()
	r.HandleFunc("/produce", ProduceHandler).Methods("POST")
	r.HandleFunc("/consume", ConsumeHandler)

	log.Fatal(http.ListenAndServe(":9988", r))
}
