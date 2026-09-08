package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	broker "yet-another-kafka/internals/broker"
	types "yet-another-kafka/internals/types"

	"github.com/gorilla/mux"
)

func main() {
	var port, brokerId int
	var zookeeper string
	flag.IntVar(&brokerId, "id", -1, "stable ID for this broker (required)")
	flag.IntVar(&port, "port", 9988, "Port for broker to run")
	flag.StringVar(&zookeeper, "zookeeper", "", "address of zookeeper service (required)")
	flag.Parse()

	switch {
	case brokerId < 0:
		log.Fatal("error: -id is required")
	case zookeeper == "":
		log.Fatal("error: -zookeeper is required")
	}

	address, err := types.GetLocalAddress(port)
	if err != nil {
		log.Fatal(err)
	}

	service, err := broker.NewService(brokerId, address, zookeeper)
	if err != nil {
		log.Fatal(err)
	}

	if err := service.RegisterWithZookeeper(); err != nil {
		log.Fatal(err)
	}

	h := broker.NewHandlers(service)

	r := mux.NewRouter()
	r.HandleFunc("/topics", h.CreateTopicHandler).Methods("POST")
	// TODO: convert this to POST /topics/<topic>/messages for produce and /consumers for consumers
	r.HandleFunc("/produce", h.ProduceHandler).Methods("POST")
	r.HandleFunc("/consume", h.ConsumeHandler).Methods("POST")
	r.HandleFunc("/health", h.HealthHandler).Methods("GET")
	r.HandleFunc("/leader", h.SetLeaderHandler).Methods("GET")

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
