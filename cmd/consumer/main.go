package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"yet-another-kafka/internals/consumer"
	"yet-another-kafka/internals/types"

	"github.com/gorilla/mux"
)

func main() {

	var topicName, zookeeper string
	var partitions, port int
	var createTopic, fromBeginning bool
	flag.StringVar(&zookeeper, "zookeeper", "", "address of zookeeper service (required)")
	flag.StringVar(&topicName, "topic", "", "name of the topic to be created (required)")
	flag.IntVar(&partitions, "partitions", 1, "partitions to create the topic with (used if create-topic is true)")
	flag.BoolVar(&createTopic, "create-topic", false, "create a new topic with given name and partitions")
	flag.BoolVar(&createTopic, "create-topic", false, "set to true if all messages from the start of topic creation needs to be read")
	flag.IntVar(&port, "port", 9988, "Port for broker to run")
	flag.Parse()

	switch {
	case topicName == "":
		log.Fatal("error: -topic is required")
	case zookeeper == "":
		log.Fatal("error: -zookeeper is required")
	}

	address, err := types.GetLocalAddress(port)
	if err != nil {
		log.Fatal(err)
	}

	service, err := consumer.NewService(address, zookeeper, topicName, fromBeginning)
	if err != nil {
		log.Fatal(err)
	}

	if createTopic {
		if err := service.CreateTopic(partitions); err != nil {
			log.Fatal(err)
		}
	}

	if err := service.RegisterConsumer(); err != nil {
		log.Fatal(err)
	}

	handlers := consumer.NewHandlers(service)

	//create server to wait for messages from broker
	r := mux.NewRouter()
	r.HandleFunc("/consume", handlers.MessageConsumeHandler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
