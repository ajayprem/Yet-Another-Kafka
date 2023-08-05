package utils

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

type RegisterBroker struct {
	Port int `json:"port"`
}

type BrokerResponse struct {
	Port int `json:"port"`
	Id   int `json:"id"`
}
