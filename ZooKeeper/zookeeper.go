package main

import (
	utils "Yet-Another-Kafka/Utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

const (
	PORT           = 9998
	MAX_FAIL_RETRY = 3
)

type state struct {
	brokers  map[int]int
	id       int
	leaderId int
	mu       sync.Mutex
}

// Return the location of the current leader Broker
func leaderLocationHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	t := -1
	if s.leaderId != -1 {
		t = s.brokers[s.leaderId]
	}
	jsonResponse, jsonError := json.Marshal(t)

	s.mu.Unlock()
	if jsonError != nil {
		fmt.Println("Unable to encode JSON")
	}
	w.Write(jsonResponse)
}

// Register a new broker onto the Cluster
func registerHandler(w http.ResponseWriter, r *http.Request) {
	var body utils.RegisterBroker
	json.NewDecoder(r.Body).Decode(&body)
	port := body.Port

	s.mu.Lock()
	s.id += 1
	s.brokers[s.id] = port

	jsonResponse, jsonError := json.Marshal(s.id)
	if jsonError != nil {
		log.Println("Unable to encode JSON")
	}
	w.WriteHeader(200)
	w.Write(jsonResponse)

	if s.id == 0 {
		log.Println("Zookeeper: New Leader elected: Broker id:", s.id)
		s.leaderId = 0
	}
	s.mu.Unlock()
}

// Elect a new leader when the current leader dies
func election() {
	// Remove the current leader
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.brokers, s.leaderId)

	log.Println("Zookeeper: Starting election")
	// Elect a new leader
	for id, location := range s.brokers {
		url := fmt.Sprintf("http://localhost:%d/health/?id=%d", location, id)

		_, err := http.Get(url)
		if err != nil {
			// Un-register dead brokers
			delete(s.brokers, id)
		} else {
			// Healthy broker found to replace Leader
			_, err := http.Get(fmt.Sprintf("http://localhost:%d/leader", location))
			if err != nil {
				log.Fatal("Zookeeper: Something went wrong")
			}
			log.Println("Zookeeper: New Leader elected: Broker id:", id)
			s.leaderId = id
			return
		}
	}

	// All brokers are dead
	log.Println("Zookeeper: All Brokers are Dead")
	s.leaderId = -1
	s.id = -1
}

func LeaderHealth() {
	count := 0
	for {
		if s.leaderId != -1 {
			url := fmt.Sprintf("http://localhost:%d/health?id=%d", s.brokers[s.leaderId], s.leaderId)

			res, err := http.Get(url)
			if err != nil || res.StatusCode != 200 {
				count += 1
				if count == MAX_FAIL_RETRY {
					log.Println("Zookeeper: Leader is Dead")
					election()
				} else {
					log.Printf("Zookeeper: Leader (id: %d) is Unhealthy: retrying", s.leaderId)
				}
			} else {
				count = 0
				log.Printf("Zookeeper: Leader (id: %d) is Alive", s.leaderId)
			}

		}
		time.Sleep(time.Second * 7)
	}
}

var s state

func main() {
	log.Println("Zookeeper: Starting on port:", PORT)
	s.brokers = make(map[int]int)
	s.id = -1
	s.leaderId = -1
	go LeaderHealth()

	r := mux.NewRouter()
	r.HandleFunc("/register", registerHandler).Methods("POST")
	r.HandleFunc("/leader", leaderLocationHandler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", PORT), r))
}
