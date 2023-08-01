// socket-server project main.go
package main

import (
	utils "Yet-Another-Kafka/Utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
	BROKER_ID   = 0
	PARTITIONS  = 0
)

type record struct{
	offset int 
	message string
}

func createTopic(topicName string, partitions int) *os.File {
	topicDir := filepath.Join("tmp", topicName + "0")
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		if err := os.Mkdir(topicDir, os.ModePerm); err != nil {
			log.Fatal(err)
		}
		file, err := os.Create(topicDir + "test.log")
		if err != nil {
			log.Fatalf("Failed creating file: %s", err)
		}
		return file;
	}
	file, err := os.OpenFile(topicDir + "test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed creating file: %s", err)
	}
	return file
}

func ProduceHandler(w http.ResponseWriter, r *http.Request) {
	var command utils.ProduceCommand
	_ = json.NewDecoder(r.Body).Decode(&command)
	fmt.Println(command.Message)
	// createTopic(command.TopicName, command.Partitions)
	w.Header().Set("Content-Type", "application/json")

}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {

}

func main() {
	createTopic("bleh", 0)
	createTopic("bleh", 0)
	fmt.Println("Starting Broker id:", BROKER_ID)
	r := mux.NewRouter()
	r.HandleFunc("/produce", ProduceHandler).Methods("POST")
	r.HandleFunc("/consume", ConsumeHandler)

	log.Fatal(http.ListenAndServe(":9988", r))
}

// func process(connection net.Conn) {

// 	for {
// 		message, err := bufio.NewReader(connection).ReadString('\n')
// 		if err != nil {
// 			log.Printf("Error: %+v", err.Error())
// 			return
// 		}

// 		log.Println("Message:", string(message))
// 	}
// }
