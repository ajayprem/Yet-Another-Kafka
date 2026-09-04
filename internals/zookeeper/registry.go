package zookeeper

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
	"yet-another-kafka/internals/retry"
)

const (
	HEALTH_CHECK_SECONDS = 7
	MAX_FAIL_RETRY       = 3
	ELECTION_BASE_DELAY  = time.Duration(100 * time.Millisecond)
	ELECTION_MAX_DELAY   = time.Duration(1 * time.Second)
)

var (
	ErrBrokerIDConflict = errors.New("broker id already registered and alive")
)

type Registry struct {
	brokers  map[int]*Broker
	leaderId int
	mu       sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		brokers:  make(map[int]*Broker),
		leaderId: -1,
	}
}

func (r *Registry) RegisterBroker(broker *Broker) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.brokers[broker.Id]; ok {
		return false, ErrBrokerIDConflict
	}

	isFirstBroker := len(r.brokers) == 0
	r.brokers[broker.Id] = broker

	if isFirstBroker {
		log.Println("zookeeper: new leader elected: broker id:", broker.Id)
		r.leaderId = broker.Id
	}

	return isFirstBroker, nil
}

func (r *Registry) GetLeader() (*Broker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.brokers) == 0 {
		return nil, false
	}
	return r.brokers[r.leaderId], true
}

func (r *Registry) GetRandomBroker() (*Broker, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.brokers) == 0 {
		return nil, false
	}
	
	// // leverage go's random map iteration to find broker
	for id, broker := range r.brokers {
		if isAlive(broker) {
			return broker, true
		} else {
			delete(r.brokers, id)
		}
	}

	return nil, false
}

func (r *Registry) LeaderHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(HEALTH_CHECK_SECONDS * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if r.needsElection() {
				r.election()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Registry) needsElection() bool {
	leader, ok := r.GetLeader()
	if !ok {
		// no conected brokers -> no election
		return false
	}
	return !isAlive(leader)
}

// TODO: improve leader election logic
func (r *Registry) election() {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Println("zookeeper: starting election")

	// remove current leader
	delete(r.brokers, r.leaderId)

	// elect new leader
	for _, broker := range r.brokers {
		if retry.Do(MAX_FAIL_RETRY, ELECTION_BASE_DELAY, ELECTION_MAX_DELAY, broker.IsAlive) {
			r.leaderId = broker.Id
			log.Println("zookeeper: new leader elected: broker id:", broker.Id)
		} else {
			delete(r.brokers, r.leaderId)
		}
	}
}

func isAlive(b *Broker) bool {
    return !retry.Do(MAX_FAIL_RETRY, ELECTION_BASE_DELAY, ELECTION_MAX_DELAY, b.IsAlive)
}