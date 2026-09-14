package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"yet-another-kafka/internals/types"
)

const (
	LEADER_SYNC_URL = "/sync"
)

type leader struct {
	address string
}

func newLeader(url string) *leader {
	return &leader{address: url}
}

func (l *leader) sync(topicOffsetList types.SyncRequest) (types.SyncResponse, error) {
	url := fmt.Sprintf("http://%s%s", l.address, LEADER_SYNC_URL)
	jsonBody, _ := json.Marshal(topicOffsetList)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(jsonBody))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return types.SyncResponse{}, fmt.Errorf("error syncing with leader:%s", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		respBody, _ := io.ReadAll(res.Body)
		return types.SyncResponse{}, fmt.Errorf("unable to syncing with broker:%s", string(respBody))
	}

	var resBody types.SyncResponse
	json.NewDecoder(res.Body).Decode(&resBody)
	return resBody, nil
}
