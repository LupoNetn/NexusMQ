package broker

import (
	"sync"
	"time"
)

type Broker interface {
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	Topics() []*Topic
	Subscribe(topic string) (*Subscriber, error)
	Publish(topic string, message *Message) error
}

type Brk struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

type Topic struct {
	mu   sync.RWMutex
	name string
	subscribers map[string]*Subscriber
}

type Subscriber struct {
	ID string
	Ch chan *Message
}

type Message struct {
	Payload   []byte
	Timestamp time.Time
}

