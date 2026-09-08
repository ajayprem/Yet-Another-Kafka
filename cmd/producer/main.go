package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"yet-another-kafka/internals/producer"
)

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
		if err := service.CreateTopic(partitions); err != nil {
			log.Fatal(err)
		}
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

		if err := service.Produce(key, value); err != nil {
			log.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("reading input: %v", err)
	}
}
