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
	Partitions int    `json:"partitions"`
	Key        string `json:"key"`
	Value      string `json:"value"`
}

type RegisterConsumer struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Port       int    `json:"port"`
}
