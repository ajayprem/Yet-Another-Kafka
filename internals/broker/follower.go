package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"yet-another-kafka/internals/types"
)

const (
	CREATE_TOPIC_PATH = "/topics"
	FOLLOW_PATH       = "/follow"
)

type follower struct {
	address string
}

func newFollower(url string) *follower {
	return &follower{address: url}
}

func (f *follower) createTopic(topicName string, partitions int) error {
	body, err := json.Marshal(types.CreateTopicRequest{TopicName: topicName, Partitions: partitions})
	if err != nil {
		return fmt.Errorf("marshal create topic message: %s", err)
	}

	url := fmt.Sprintf("http://%s%s", f.address, CREATE_TOPIC_PATH)
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error sending message to folower: %s", err)
	}
	defer res.Body.Close()

	_, _ = io.Copy(io.Discard, res.Body)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("error message from consumer: %s, status(%d)", respBody, res.StatusCode)
	}

	return nil
}

func (f *follower) apply(topicName string, partitions int, msg types.Message) (int, error) {
	body, err := json.Marshal(types.FollowMessageRequest{TopicName: topicName, Partitions: partitions, Key: msg.Key, Value: msg.Value, Offset: msg.Offset})
	if err != nil {
		return 0, fmt.Errorf("marshal follow message: %s", err)
	}

	url := fmt.Sprintf("http://%s%s", f.address, FOLLOW_PATH)
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("error sending message to follower: %s", err)
	}
	defer res.Body.Close()

	_, _ = io.Copy(io.Discard, res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		respBody, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("error message from follower: %s, status(%d)", respBody, res.StatusCode)
	}

	var resBody types.FollowMessageResponse
	json.NewDecoder(res.Body).Decode(&resBody)
	return resBody.LastOffset, nil
}

type backfillRequest struct {
	follower   *follower
	topicName  string
	lastOffset int
}

type followerStore struct {
	mu         sync.RWMutex
	followers  []*follower
	backfillCh chan backfillRequest
}

func newFollowerStore() *followerStore {
	return &followerStore{
		followers:  make([]*follower, 0),
		backfillCh: make(chan backfillRequest, 100),
	}
}

// TODO: improve error handling + duplicates handling
func (fs *followerStore) addFolower(f *follower) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.followers = append(fs.followers, f)
}

func (fs *followerStore) createTopic(topicName string, partitions int) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for _, follower := range fs.followers {
		if err := follower.createTopic(topicName, partitions); err != nil {
			log.Printf("error creating topic on follower(%s):%s", follower.address, err)
		}
	}
}

func (fs *followerStore) applyMessage(topicName string, partitions int, msg types.Message) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for _, follower := range fs.followers {
		lastOffset, err := follower.apply(topicName, partitions, msg)
		if err != nil {
			log.Printf("error applying message on follower(%s):%s", follower.address, err)
		} else if lastOffset != msg.Offset {
			fs.requestBackfil(follower, topicName, lastOffset)
		}
	}
}

func (fs *followerStore) requestBackfil(follower *follower, topicName string, lastOffset int) {
	req := backfillRequest{follower: follower, topicName: topicName, lastOffset: lastOffset}
	select {
	case fs.backfillCh <- req:
	default:
		log.Printf("backfill channel full, dropping request for follower(%s) topic(%s)", follower.address, topicName)
	}
}
