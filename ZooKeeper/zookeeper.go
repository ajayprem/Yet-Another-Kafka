package main

import (
	utils "Yet-Another-Kafka/Utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const (
	PORT = 9998
)

var (
	brokers  = make(map[int]int)
	id       = -1
	leaderId = -1
)

func registerHandler(w http.ResponseWriter, r *http.Request) {
	var body utils.RegisterBroker
	json.NewDecoder(r.Body).Decode(&body)
	port := body.Port

	id += 1
	brokers[id] = port
	if id == 0 {
		leaderId = 0
	}

	jsonResponse, jsonError := json.Marshal(id)
	if jsonError != nil {
		fmt.Println("Unable to encode JSON")
	}
	w.WriteHeader(200)
	w.Write(jsonResponse)
}

func election() {
	// Remove the current leader
	delete(brokers, leaderId)

	log.Println("Zookeeper: Starting election")
	// Elect a new leader
	for id, location := range brokers {
		url := fmt.Sprintf("http://localhost:%d/health", location)

		_, err := http.Get(url)
		if err != nil {
			// Un-register dead brokers
			delete(brokers, id)
		} else {
			// Healthy broker found to replace Leader
			log.Println("Zookeeper: New Leader elected: Broker id:", id)
			leaderId = id
			return
		}
	}

	// All brokers are dead
	log.Println("Zookeeper: All Brokers are Dead")
	leaderId = -1
	id = -1
}

func LeaderHealth() {
	for {
		if leaderId != -1 {
			url := fmt.Sprintf("http://localhost:%d/health", brokers[leaderId])

			_, err := http.Get(url)
			if err != nil {
				log.Println("Zookeeper: Leader is Dead")
				election()
			} else {
				log.Println("Zookeeper: Leader is Alive")
			}

		}
		time.Sleep(time.Second * 5)
	}
}

func main() {
	log.Println("Zookeeper: Starting on port:", PORT)
	go LeaderHealth()

	r := mux.NewRouter()
	r.HandleFunc("/register", registerHandler).Methods("POST")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", PORT), r))
}
