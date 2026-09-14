package types

type RegisterBrokerRequest struct {
	Id      int    `json:"id"`
	Address string `json:"address"`
	// for followers if they have already synced with the leader
	Synced bool `json:"synced"`
}

type RegisterBrokerResponse struct {
	IsLeader      bool   `json:"is_leader"`
	LeaderAddress string `json:"leader_address"`
}

type BrokerAddressResponse struct {
	Address string `json:"address"`
}

type CreateTopicRequest struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
}

type ProduceMessageRequest struct {
	TopicName string `json:"topic_name"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type FollowMessageRequest struct {
	TopicName string `json:"topic_name"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Offset    int    `json:"offset"`
}

type FollowMessageResponse struct {
	LastOffset int `json:"last_offset"`
}

type RegisterConsumerRequest struct {
	TopicName     string `json:"topic_name"`
	Address       string `json:"address"`
	FromBeginning bool   `json:"from_beginning"`
}

type ConsumerMessageData struct {
	Offset int    `json:"offset"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

type TopicOffset struct {
	TopicName string `json:"topic_name"`
	Offset    int    `json:"offset"`
}

type SyncRequest struct {
	TopicOffsetList []TopicOffset `json:"topic_offset_list"`
}

type TopicMessage struct {
	TopicName string    `json:"topic_name"`
	Messages  []Message `json:"message_list"`
}

type SyncResponse struct {
	TopicMessageList []TopicMessage `json:"topic_message_list"`
}
