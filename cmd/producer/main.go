package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"yet-another-kafka/internals/producer"
)

var (
	zookeeperURL = fmt.Sprintf("http://localhost:%d/leader", 9998)
)

func connectToBroker() int {
	// Connect to zookeeper to find the leader broker
	var port int
	count := 0
	for count < 5 {
		res, err := http.Get(zookeeperURL)
		if err != nil {
			log.Fatalf("Producer: Unable to connect to Zookeeper to find the leader: %s\n", err)
		}

		json.NewDecoder(res.Body).Decode(&port)
		if port != -1 {
			return port
		}
		log.Println("Producer: Unable to connect to leader broker: Retrying")
		time.Sleep(time.Second * 5)
		count += 1
	}
	log.Fatalf("Producer: Leader broker unavailable")
	return 0
}

func main() {
	var topicName, zookeeper string
	var partitions int
	var createTopic bool
	flag.StringVar(&zookeeper, "zookeeper", "", "address of zookeeper service (required)")
	flag.StringVar(&topicName, "topic", "", "name of the topic to be created (required)")
	flag.IntVar(&partitions, "partitions", 1, "partitions to create the topic with (used if create-topic is true)")
	flag.BoolVar(&createTopic, "create-topic", false, "create a new topic with given name and partitions")
	flag.Parse()

	switch {
	case topicName == "":
		log.Fatal("error: -topic is required")
	case zookeeper == "":
		log.Fatal("error: -zookeeper is required")
	}

	service, err := producer.NewService(zookeeper, topicName)
	if err != nil {
		log.Fatal(err)
	}

	if createTopic {
		service.CreateTopic(partitions)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		var key, value string
		if len(parts) == 2 {
			key = strings.TrimSpace(parts[0])
			value = strings.TrimSpace(parts[1])
		} else {
			// no delimiter found — treat the whole line as the value, empty key
			value = strings.TrimSpace(parts[0])
		}

		service.Produce(key, value)
	}
}
