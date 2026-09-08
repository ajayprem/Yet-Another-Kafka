package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	types "yet-another-kafka/internals/types"
)

const (
	DEFAULT_PARTITIONS = 0
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
		return nil, err
	}

	logStore.scanExistingTopics(metadataStore.addTopicMetadata)

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

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)
	req, _ := http.NewRequest(http.MethodPost, s.zookeeperURL, bodyReader)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("broker: error connecting with zookeeper: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("broker: unable to register with zookeeper, status code: %d", res.StatusCode)
	}

	var resBody types.RegisterBrokerResponse
	json.NewDecoder(res.Body).Decode(&resBody)

	s.isLeader = resBody.IsLeader
	return nil
}

func (s *Service) setLeader() {
	s.isLeader = true
}

func (s *Service) createTopic(topicName string, partitions int) error {
	if s.metadataStore.doesTopicExist(topicName) {
		return fmt.Errorf("Service.CreateTopic: topic name already exists")
	}

	// offset starts at -1
	if err := s.logStore.createTopicFiles(topicName, partitions); err != nil {
		return err
	}
	s.metadataStore.addTopicMetadata(topicName, partitions, -1)
	return nil
}

func (s *Service) produceMessage(topicName string, msg types.Message) error {
	if !s.metadataStore.doesTopicExist(topicName) {
		return fmt.Errorf("service.produceMessage: topic(%s) does not exist", topicName)
	}

	partition, offset, err := s.metadataStore.nextOffset(topicName, msg.Key)
	if err != nil {
		return err
	}

	return s.logStore.appendRecord(topicName, partition, offset, msg)
}

func (s *Service) registerConsumer(topicName, consumerURL string, fromBegin bool) error {
	c := &consumer{consumerURL}
	s.consumerStore.addConsumer(c, topicName)
	if fromBegin {
		err := s.logStore.scanTopicFiles(topicName, c.sendMessage)
		if err != nil {
			return fmt.Errorf("service.addConsumer: %s", err)
		}
	}
	return nil
}
