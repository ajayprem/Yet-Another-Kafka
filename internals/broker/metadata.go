package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type topicMetaData struct {
	partitions int
	offset     int
}

type metadataStore struct {
	mu     sync.RWMutex
	topics map[string]*topicMetaData
}

func newTopicMetaData() *metadataStore {
	return &metadataStore{
		topics: make(map[string]*topicMetaData),
	}
}

func (m *metadataStore) doesTopicExist(topicName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.topics[topicName]
	return ok
}

func (m *metadataStore) addTopicMetadata(topicName string, partitions, offset int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[topicName] = &topicMetaData{partitions: partitions, offset: offset}
}

func (m *metadataStore) nextOffset(topicName, key string) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta, ok := m.topics[topicName]
	if !ok {
		return 0, 0, fmt.Errorf("metadata: topic %q does not exist", topicName)
	}

	partition := getPartition(key, meta.partitions)
	meta.offset++
	return partition, meta.offset, nil
}

func getPartition(key string, partitionCount int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(partitionCount))
}
