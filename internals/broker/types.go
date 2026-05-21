package broker

import (
	"sync"
	"time"
)

type Broker interface {
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	Topics() []*Topic
	Subscribe(topic string) (Subscriber, error)
	Unsubscribe(topicName string, subID string) error
	Publish(topic string, message *Message) error
	Shutdown() error
}

type Brk struct {
	mu       sync.RWMutex
	topics   map[string]*Topic
	shutdown bool
}

type Topic struct {
	mu   sync.RWMutex
	name string
	subscribers map[string]*subscription
}

type Subscriber interface {
	Receive() (*Message, error)
}

type subscription struct {
	ID string
	Ch chan *Message
}

type Message struct {
	Payload   []byte
	Timestamp time.Time
}
