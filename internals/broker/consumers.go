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
	mu               sync.Mutex
	topicConsumerMap map[string][]*consumer
}

func newConsumerStore() *consumerStore {
	return &consumerStore{
		topicConsumerMap: make(map[string][]*consumer),
	}
}

// TODO: improve error handling
func (cs *consumerStore) addConsumer(c *consumer, topicname string) {
	cs.mu.Lock()
	consumers, ok := cs.topicConsumerMap[topicname]
	if !ok {
		cs.topicConsumerMap[topicname] = make([]*consumer, 0)
	} else {
		consumers = append(consumers, c)
	}
}
