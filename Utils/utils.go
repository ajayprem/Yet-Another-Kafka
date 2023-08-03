package utils

type ProduceCommand struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Message    string `json:"message"`
}

type RegisterCommand struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Port       int    `json:"port"`
}
