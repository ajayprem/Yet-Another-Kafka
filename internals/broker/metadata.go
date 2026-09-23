package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"yet-another-kafka/internals/types"
)

type topicMetaData struct {
	partitions int
	// real kafka uses offset per partition and not one global offset, maybe we can support that later
	offset    int
	topicLock sync.RWMutex
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

func (m *metadataStore) getTopicMetadata(topicName string) (*topicMetaData, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	topicMetadata, ok := m.topics[topicName]
	return topicMetadata, ok
}

func (m *metadataStore) getTopicOffset(topicName string) (int, bool) {
	meta, ok := m.getTopicMetadata(topicName)
	if !ok {
		return 0, ok
	}

	meta.topicLock.RLock()
	defer meta.topicLock.RUnlock()
	return meta.offset, true
}

func (m *metadataStore) getTopicPartitions(topicName string) int {
	meta, _ := m.getTopicMetadata(topicName)
	return meta.partitions
}

func (m *metadataStore) addTopicMetadata(topicName string, partitions, offset int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[topicName] = &topicMetaData{partitions: partitions, offset: offset}
}

func (m *metadataStore) generateSyncRequest() map[string]int {
	result := make(map[string]int)
	m.mu.Lock()
	defer m.mu.Unlock()
	for topicName, meta := range m.topics {
		result[topicName] = meta.offset
	}
	return result
}

func (m *metadataStore) getAllTopicNamesNotInMap(topicOffsetMap map[string]int) map[string]int {
	m.mu.RLock()
	m.mu.Unlock()

	var topicPartitionMap map[string]int
	for topicName, meta := range m.topics {
		if _, ok := topicOffsetMap[topicName]; !ok {
			topicPartitionMap[topicName] = meta.partitions
		}
	}

	return topicPartitionMap
}

type opFunc func(topicName string, partition, offset int, msg types.Message) error

func (m *metadataStore) incrementOffset(topicName string, msg types.Message, op opFunc) error {
	meta, ok := m.getTopicMetadata(topicName)
	if !ok {
		return fmt.Errorf("metadata: topic %q does not exist", topicName)
	}

	meta.topicLock.Lock()
	defer meta.topicLock.Unlock()

	partition := getPartition(msg.Key, meta.partitions)
	nextOffset := meta.offset + 1
	if err := op(topicName, partition, nextOffset, msg); err != nil {
		return err
	}
	meta.offset = nextOffset
	return nil
}

func (m *metadataStore) incrementOffsetIfExpected(topicName string, msg types.Message, op opFunc) (int, error) {
	meta, ok := m.getTopicMetadata(topicName)
	if !ok {
		return 0, fmt.Errorf("metadata: topic %q does not exist", topicName)
	}

	meta.topicLock.Lock()
	defer meta.topicLock.Unlock()

	partition := getPartition(msg.Key, meta.partitions)
	nextOffset := meta.offset + 1
	if nextOffset != msg.Offset {
		return meta.offset, nil
	}
	if err := op(topicName, partition, nextOffset, msg); err != nil {
		return 0, err
	}
	meta.offset = nextOffset
	return nextOffset, nil
}

type readLockFunc func() error

func (m *metadataStore) withReadLock(topicName string, op readLockFunc) error {
	meta, ok := m.getTopicMetadata(topicName)
	if !ok {
		return fmt.Errorf("metadata: topic %q does not exist", topicName)
	}

	meta.topicLock.RLock()
	defer meta.topicLock.RUnlock()

	return op()
}

// Can be improved to send only the hash of key to reduce lock hold time
func getPartition(key string, partitionCount int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(partitionCount))
}
