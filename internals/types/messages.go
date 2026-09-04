package types

type ProduceMessage struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Message    string `json:"message"`
}

type RegisterConsumer struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Port       int    `json:"port"`
}

type Broker struct {
	Address string `json:"address"`
	Id      int    `json:"id"`
}

type BrokerResponse struct {
	Port int `json:"port"`
	Id   int `json:"id"`
}

type RegisterBrokerResponse struct {
	IsLeader bool `json:"is_leader"`
}

type BrokerAddressResponse struct {
	Address string `json:"address"`
}