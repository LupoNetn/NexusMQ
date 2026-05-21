package broker

import (
	"errors"
)

var (
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrPublishTimeout     = errors.New("publish timeout on one or more subscribers")
	ErrBrokerShutdown     = errors.New("broker is shut down")
)

type OverflowError struct {
	SubscriberID string
	Topic        string
	DroppedCount int
}

func (e *OverflowError) Error() string {
	return "subscriber " + e.SubscriberID + " on topic " + e.Topic + " dropped messages due to overflow"
}