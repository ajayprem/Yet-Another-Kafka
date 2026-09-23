package broker

import (
	"fmt"
	"log"
	"sync"
	"time"
	"yet-another-kafka/internals/retry"
	types "yet-another-kafka/internals/types"
)

const (
	REGISTER_SYNC_MAX_RETRY  = 3
	REGISTER_SYNC_BASE_DELAY = time.Duration(100 * time.Millisecond)
	REGISTER_SYNC_MAX_DELAY  = time.Duration(1 * time.Second)
)

type Service struct {
	id            int
	address       string
	isLeader      bool
	leader        *leader
	zookeeper     *zookeeper
	metadataStore *metadataStore
	logStore      *logStore
	followerStore *followerStore
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
		zookeeper:     newZookeeper(zookeeperURL),
		metadataStore: metadataStore,
		logStore:      logStore,
		followerStore: newFollowerStore(),
		consumerStore: newConsumerStore(),
	}, nil
}

// Register the broker with zookeeper
// First request without sync, if leader then stop here
// If not leader then sync with leader and re-register with zookeeper
func (s *Service) RegisterWithZookeeper() error {
	if !retry.Do(REGISTER_SYNC_MAX_RETRY, REGISTER_SYNC_BASE_DELAY, REGISTER_SYNC_MAX_DELAY, func() bool {
		// register with zookeeper to find the leader
		isLeader, leaderAddress, err := s.zookeeper.registerBroker(s.id, s.address, false)
		if err != nil {
			log.Printf("error registering broker:%s", err)
			return false
		}
		if isLeader {
			s.isLeader = true
			return true
		}

		// if broker is not the leader, then get all logs from the leader from our current state
		s.leader = newLeader(leaderAddress)
		if err := s.syncWithLeader(); err != nil {
			log.Printf("error syncing with leader(%s):%s", s.leader.address, err)
			return false
		}

		return true
	}) {
		return fmt.Errorf("unable to register broker with zookeeper")
	}

	// final register after sync with leader
	if !s.isLeader {
		isLeader, leaderAddress, err := s.zookeeper.registerBroker(s.id, s.address, true)
		if err != nil {
			log.Printf("error registering with zookeeper after sync:%s", err)
			return fmt.Errorf("error registering with zookeeper after sync")
		}
		s.isLeader = isLeader
		s.leader = newLeader(leaderAddress)
	}

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
	if s.isLeader {
		go s.followerStore.createTopic(topicName, partitions)
	}
	return nil
}

// leader function to produce messages, notify followers as well as consumners
func (s *Service) produceMessage(topicName string, msg types.Message) error {
	if !s.metadataStore.doesTopicExist(topicName) {
		return fmt.Errorf("topic(%s) does not exist", topicName)
	}

	// increment offset and log append as one atomic operation
	if err := s.metadataStore.incrementOffset(topicName, msg, s.logStore.appendRecord); err != nil {
		log.Printf("error appending record:%s", err)
		return fmt.Errorf("unable to produce record")
	}

	go s.consumerStore.notifyConsumers(topicName, msg)
	go s.followerStore.applyMessage(topicName, s.metadataStore.getTopicPartitions(topicName), msg)
	return nil
}

// follower function to apply messages produced by the leader
func (s *Service) apply(topicName string, partitions int, msg types.Message) (int, error) {
	if !s.metadataStore.doesTopicExist(topicName) {
		s.createTopic(topicName, partitions)
	}

	// increment offset and log append as one atomic operation
	lastOffset, err := s.metadataStore.incrementOffsetIfExpected(topicName, msg, s.logStore.appendRecord)
	if err != nil {
		log.Printf("error appending record:%s", err)
		return 0, fmt.Errorf("unable to produce record")
	}

	if lastOffset == msg.Offset {
		go s.consumerStore.notifyConsumers(topicName, msg)
	}
	return lastOffset, nil
}

// any broker can get the following request to register consumers
// send all messages for consumers with fromBeginning set to true
func (s *Service) registerConsumer(topicName, consumerURL string, fromBegin bool) error {
	c := &consumer{consumerURL}
	s.consumerStore.addConsumer(c, topicName)
	if fromBegin {
		var allMessages []types.Message
		s.metadataStore.withReadLock(topicName, func() error {
			messages, err := s.logStore.getMessagesFromOffset(topicName, 0)
			if err != nil {
				return err
			}
			allMessages = messages
			return nil
		})

		for _, message := range allMessages {
			c.sendMessage(message)
		}
	}
	return nil
}

// sync section
// this process takes place during register with zookeeper

func (s *Service) syncWithLeader() error {
	topicMessages, err := s.leader.sync(s.address, s.metadataStore.generateSyncRequest())
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, t := range topicMessages {
		wg.Add(1)
		go s.syncApplyMessages(t.TopicName, t.Partitions, t.Messages)
	}
	wg.Wait()
	return nil
}

// TODO: currently implemented by using existing append method, but this wastes CPU as a file
// is repeatedly opened and closed for long messages.
func (s *Service) syncApplyMessages(topicName string, partitions int, messages []types.Message) {
	if !s.metadataStore.doesTopicExist(topicName) {
		s.createTopic(topicName, partitions)
	}
	for _, msg := range messages {
		// increment offset and log append as one atomic operation
		if err := s.metadataStore.incrementOffset(topicName, msg, s.logStore.appendRecord); err != nil {
			log.Printf("error appending record:%s", err)
			break
		}
	}
}

// send messages from a given offset to the follower
func (s *Service) syncMessages(followerAddress string, topicOffsetMap map[string]int) (types.SyncResponse, error) {
	var result types.SyncResponse

	for topicName, offset := range topicOffsetMap {
		var messagesFromOffset []types.Message
		if err := s.metadataStore.withReadLock(topicName, func() error {
			messages, err := s.logStore.getMessagesFromOffset(topicName, offset+1)
			if err != nil {
				return err
			}
			messagesFromOffset = messages
			return nil
		}); err != nil {
			log.Printf("error sycning messages:%s", err)
			return types.SyncResponse{}, fmt.Errorf("error syncing messages")
		}
		result.TopicMessageList = append(result.TopicMessageList, types.TopicMessage{TopicName: topicName, Messages: messagesFromOffset})
	}

	for topicName, partitions := range s.metadataStore.getAllTopicNamesNotInMap(topicOffsetMap) {
		var messagesFromOffset []types.Message
		if err := s.metadataStore.withReadLock(topicName, func() error {
			messages, err := s.logStore.getMessagesFromOffset(topicName, 0)
			if err != nil {
				return err
			}
			messagesFromOffset = messages
			return nil
		}); err != nil {
			log.Printf("error sycning messages:%s", err)
			return types.SyncResponse{}, fmt.Errorf("error syncing messages")
		}
		result.TopicMessageList = append(result.TopicMessageList, types.TopicMessage{TopicName: topicName, Messages: messagesFromOffset, Partitions: partitions})
	}

	s.followerStore.addFolower(newFollower(followerAddress))
	return result, nil
}

func (s *Service) logf(format string, args ...any) {
	prefix := fmt.Sprintf("[brokerId:%d isLeader:%t] ", s.id, s.isLeader)
	log.Printf(prefix+format, args...)
}

// backfill section
// on gap detection, requests are sent to leader to backfill the follower from the missing messages

func (s *Service) RunBackfillWorker() {
	for req := range s.followerStore.backfillCh {
		s.backfillFollower(req)
	}
}

func (s *Service) backfillFollower(req backfillRequest) {
	var messagesFromOffset []types.Message

	if err := s.metadataStore.withReadLock(req.topicName, func() error {
		messages, err := s.logStore.getMessagesFromOffset(req.topicName, req.lastOffset+1)
		if err != nil {
			return err
		}
		messagesFromOffset = messages
		return nil
	}); err != nil {
		log.Printf("error sycning messages:%s", err)
		return
	}

	for _, message := range messagesFromOffset {
		lastOffset, err := req.follower.apply(req.topicName, s.metadataStore.getTopicPartitions(req.topicName), message)
		if err != nil {
			log.Printf("error backfilling follower(%s):%s", req.follower.address, err)
			return
		}
		if lastOffset != message.Offset {
			log.Printf("stopping backfill for follower(%s), since offset mismatch", req.follower.address)
			return
		}
	}
}
