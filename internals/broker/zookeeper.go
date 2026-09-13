package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"yet-another-kafka/internals/types"
)

const (
	ZOOKEEPER_BROKER_REGISTER_PATH = "/brokers"
)

type zookeeper struct {
	url string
}

func newZookeeper(url string) *zookeeper {
	return &zookeeper{url: url}
}

func (z *zookeeper) registerBroker(brokerId int, brokerAddress string, synced bool) (bool, string, error) {
	body := types.RegisterBrokerRequest{Id: brokerId, Address: brokerAddress, Synced: synced}
	url := fmt.Sprintf("http://%s%s", z.url, ZOOKEEPER_BROKER_REGISTER_PATH)

	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, bodyReader)
	if err != nil {
		return false, "", fmt.Errorf("error creating request: %s", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, "", fmt.Errorf("error connecting with zookeeper: %s", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return false, "", fmt.Errorf("unable to register with zookeeper, status code: %d", res.StatusCode)
	}
	var resBody types.RegisterBrokerResponse
	json.NewDecoder(res.Body).Decode(&resBody)

	return resBody.IsLeader, resBody.LeaderAddress, nil
}
