package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"yet-another-kafka/internals/types"
)

const (
	CONSUMER_PATH = "/messages"
)

type consumer struct {
	address string
}

func (c *consumer) sendMessage(msg types.Message) error {
	url := fmt.Sprintf("http://%s%s", c.address, CONSUMER_PATH)
	body, err := json.Marshal(types.ConsumerMessageData{Offset: msg.Offset, Key: msg.Key, Value: msg.Value})
	if err != nil {
		return fmt.Errorf("consumers.sendMessage: marshal consumer message: %w", err)
	}

	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("consumers.sendMessage: error sending message to consumer: %w", err)
	}
	defer res.Body.Close()

	_, _ = io.Copy(io.Discard, res.Body)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("consumer returned status %s", res.Status)
	}

	return nil
}

type consumerStore struct {
	mu               sync.RWMutex
	topicConsumerMap map[string][]*consumer
}

func newConsumerStore() *consumerStore {
	return &consumerStore{
		topicConsumerMap: make(map[string][]*consumer),
	}
}

// TODO: improve error handling
func (cs *consumerStore) addConsumer(c *consumer, topicName string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	consumers, ok := cs.topicConsumerMap[topicName]
	if !ok {
		cs.topicConsumerMap[topicName] = make([]*consumer, 0)
	} else {
		consumers = append(consumers, c)
	}
}

func (cs *consumerStore) notifyConsumers(topicName string, msg types.Message) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if consumers, ok := cs.topicConsumerMap[topicName]; ok {
		for _, c := range consumers {
			if err := c.sendMessage(msg); err != nil {
				return fmt.Errorf("error notifying consumers: %s", err)
			}
		}
	}
	return nil
}
