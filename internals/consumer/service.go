package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"yet-another-kafka/internals/types"
)

const (
	ZOOKEEPER_LEADER_PATH         = "/leader"
	ZOOKEEPER_BROKER_PATH         = "/brokers"
	BROKER_CONSUMER_REGISTER_PATH = "/consume"
	BROKER_CREATE_TOPIC_PATH      = "/topics"
)

type Service struct {
	address       string
	zookeeperURL  string
	leaderURL     string
	brokerURL     string
	topicName     string
	fromBeginning bool
}

func NewService(address, zookeeperURL, topicName string, fromBeginning bool) (*Service, error) {
	s := &Service{address: address, zookeeperURL: zookeeperURL, topicName: topicName, fromBeginning: fromBeginning}
	brokerURL, err := s.getBrokerAddress(ZOOKEEPER_BROKER_PATH)
	if err != nil {
		return nil, fmt.Errorf("error while getting leader broker:%s", err)
	}
	s.brokerURL = brokerURL
	return s, nil
}

func (s *Service) CreateTopic(partitions int) error {
	leaderURL, err := s.getBrokerAddress(ZOOKEEPER_LEADER_PATH)
	if err != nil {
		return fmt.Errorf("error creating topic: %s", err)
	}

	url := fmt.Sprintf("http://%s%s", s.brokerURL, leaderURL)
	body := types.CreateTopicRequest{TopicName: s.topicName, Partitions: partitions}
	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, _ := http.NewRequest(http.MethodPost, url, bodyReader)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error connecting with leader broker:%s", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unable to register with leader broker:%s", string(respBody))
	}

	return nil
}

func (s *Service) RegisterConsumer() error {
	url := fmt.Sprintf("http://%s%s", s.brokerURL, BROKER_CONSUMER_REGISTER_PATH)
	body := types.RegisterConsumerRequest{TopicName: s.topicName, Address: s.address, FromBeginning: s.fromBeginning}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(jsonBody))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error connecting with broker:%s", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unable to register with broker:%s", string(respBody))
	}

	return nil
}

func (s *Service) consume(msg types.Message) {
	fmt.Printf("> offset=%d key=%q value=%q\n",
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}

func (s *Service) getBrokerAddress(path string) (string, error) {
	url := fmt.Sprintf("http://%s%s", s.zookeeperURL, path)
	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	var resBody types.BrokerAddressResponse
	json.NewDecoder(res.Body).Decode(&resBody)
	return resBody.Address, nil
}
