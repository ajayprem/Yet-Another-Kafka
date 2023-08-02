package utils

type ProduceCommand struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
	Message    string `json:"message"`
}

type ConsumeCommand struct {
	TopicName  string `json:"topic_name"`
	Partitions int    `json:"partitions"`
}
