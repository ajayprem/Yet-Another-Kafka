package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	types "yet-another-kafka/internals/types"
)

const (
	DEFAULT_PARTITIONS             = 0
	ZOOKEEPER_BROKER_REGISTER_PATH = "/brokers"
)

type Service struct {
	id            int
	address       string
	isLeader      bool
	zookeeperURL  string
	metadataStore *metadataStore
	logStore      *logStore
	consumerStore *consumerStore
}

// TODO: we might not need a lot of these fields
func NewService(id int, address string, zookeeperURL string) (*Service, error) {
	metadataStore := newTopicMetaData()

	logStore, err := newLogStore(id)
	if err != nil {
		return nil, fmt.Errorf("error creating service: %s", err)
	}

	if err := logStore.scanExistingTopics(metadataStore.addTopicMetadata); err != nil {
		return nil, fmt.Errorf("error creating service: %s", err)
	}

	return &Service{
		id:            id,
		address:       address,
		isLeader:      false,
		zookeeperURL:  zookeeperURL,
		metadataStore: metadataStore,
		logStore:      logStore,
		consumerStore: newConsumerStore(),
	}, nil
}

// Register the broker with zookeeper, if calls to zookeeper fail
func (s *Service) RegisterWithZookeeper() error {
	body := types.RegisterBrokerRequest{Id: s.id, Address: s.address}
	url := fmt.Sprintf("http://%s%s", s.zookeeperURL, ZOOKEEPER_BROKER_REGISTER_PATH)

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, bodyReader)
	if err != nil {
		return fmt.Errorf("error creating request: %s", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error connecting with zookeeper: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("unable to register with zookeeper, status code: %d", res.StatusCode)
	}

	var resBody types.RegisterBrokerResponse
	json.NewDecoder(res.Body).Decode(&resBody)

	s.isLeader = resBody.IsLeader
	s.logf("successfully registered broker")
	return nil
}

func (s *Service) setLeader() {
	s.isLeader = true
}

func (s *Service) createTopic(topicName string, partitions int) error {
	if s.metadataStore.doesTopicExist(topicName) {
		return fmt.Errorf("topic name already exists")
	}

	if err := s.logStore.createTopicFiles(topicName, partitions); err != nil {
		log.Printf("error creating topic files:%s", err)
		return fmt.Errorf("unable to create topic log files")
	}
	s.metadataStore.addTopicMetadata(topicName, partitions, -1)
	s.logf("created topic:%s with partitions:%d", topicName, partitions)
	return nil
}

func (s *Service) produceMessage(topicName string, msg types.Message) error {
	if !s.metadataStore.doesTopicExist(topicName) {
		return fmt.Errorf("topic(%s) does not exist", topicName)
	}

	partition, offset, _ := s.metadataStore.nextOffset(topicName, msg.Key)

	if err := s.logStore.appendRecord(topicName, partition, offset, msg); err != nil {
		log.Printf("error appending record:%s", err)
		return fmt.Errorf("unable to produce record")
	}

	go s.consumerStore.notifyConsumers(topicName, msg)
	return nil
}

func (s *Service) registerConsumer(topicName, consumerURL string, fromBegin bool) error {
	c := &consumer{consumerURL}
	s.consumerStore.addConsumer(c, topicName)
	if fromBegin {
		err := s.logStore.scanTopicFiles(topicName, c.sendMessage)
		if err != nil {
			log.Printf("error scanning topic files: %s", err)
			return fmt.Errorf("unable to scan existing records from log files")
		}
	}
	return nil
}

func (s *Service) logf(format string, args ...any) {
	prefix := fmt.Sprintf("[brokerId:%d isLeader:%t] ", s.id, s.isLeader)
	log.Printf(prefix+format, args...)
}
