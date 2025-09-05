package mq

type PCConfig struct {
	ConsumerName string
	Queue        string
	RoutingKey   string
	Exchange     string
	Concurrency  int
}
