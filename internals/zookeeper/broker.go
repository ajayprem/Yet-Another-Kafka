package zookeeper

import (
	"fmt"
	"net/http"
)

const (
	HEALTH_CHECK_PATH = "/health"
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