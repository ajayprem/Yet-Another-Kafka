package producer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"yet-another-kafka/internals/types"
)

const (
	ZOOKEEPER_LEADER_PATH    = "/leader"
	BROKER_PRODUCE_PATH      = "/produce"
	BROKER_CREATE_TOPIC_PATH = "/topics"
)

type Service struct {
	zookeeperURL string
	brokerURL    string
	topicName    string
}

func NewService(zookeeperURL, topicName string) (*Service, error) {
	s := &Service{zookeeperURL: zookeeperURL, topicName: topicName}
	if err := s.getLeaderBrokerAddress(); err != nil {
		return nil, fmt.Errorf("service.NewService error while getting leader broker: %s", err)
	}
	return s, nil
}

func (s *Service) getLeaderBrokerAddress() error {
	url := fmt.Sprintf("http://%s%s", s.zookeeperURL, ZOOKEEPER_LEADER_PATH)
	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var resBody types.BrokerAddressResponse
	json.NewDecoder(res.Body).Decode(&resBody)

	s.brokerURL = resBody.Address
	return nil
}

func (s *Service) CreateTopic(partitions int) error {
	url := fmt.Sprintf("http://%s%s", s.brokerURL, BROKER_CREATE_TOPIC_PATH)
	body := types.CreateTopicRequest{TopicName: s.topicName, Partitions: partitions}
	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, _ := http.NewRequest(http.MethodPost, url, bodyReader)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("service.CreateTopic: error connecting with zookeeper: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("service.CreateTopic: unable to register with zookeeper, status code: %d", res.StatusCode)
	}

	return nil
}

func (s *Service) Produce(key, value string) error {
	url := fmt.Sprintf("http://%s%s", s.brokerURL, BROKER_PRODUCE_PATH)
	body := types.ProduceMessageRequest{TopicName: s.topicName, Key: key, Value: value}
	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, _ := http.NewRequest(http.MethodPost, url, bodyReader)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("service.CreateTopic: error connecting with zookeeper: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("service.CreateTopic: unable to register with zookeeper, status code: %d", res.StatusCode)
	}

	return nil
}
