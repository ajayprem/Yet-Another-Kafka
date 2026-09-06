package zookeeper

import (
	"fmt"
	"net/http"
)

const (
	HEALTH_CHECK_PATH = "/health"
	SET_LEADER_PATH   = "/leader"
)

type Broker struct {
	Id      int
	Address string
}

func (b *Broker) IsAlive() bool {
	url := fmt.Sprintf("http://%s%s", b.Address, HEALTH_CHECK_PATH)
	res, err := http.Get(url)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == http.StatusOK
}

func (b *Broker) SetLeader() bool {
	url := fmt.Sprintf("http://%s%s", b.Address, SET_LEADER_PATH)
	res, err := http.Post(url, "application/json", nil)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == http.StatusOK
}
