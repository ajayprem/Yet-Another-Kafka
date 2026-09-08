package types

type RegisterBrokerRequest struct {
	Id      int    `json:"id"`
	Address string `json:"address"`
}

type RegisterBrokerResponse struct {
	IsLeader bool `json:"is_leader"`
}

type BrokerAddressResponse struct {
	Address string `json:"address"`
}

type CreateTopicRequest struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
}

type ProduceMessageRequest struct {
	TopicName  string `json:"topic_name"`
	Key        string `json:"key"`
	Value      string `json:"value"`
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
