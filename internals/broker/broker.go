package broker

import (
	"fmt"
	"sync"
	"time"
)

func NewBroker() Broker {
	return &Brk{
		mu:     sync.RWMutex{},
		topics: make(map[string]*Topic),
	}
}

func NewTopic(name string) *Topic {
	return &Topic{
		mu:          sync.RWMutex{},
		name:        name,
		subscribers: make(map[string]*Subscriber),
	}
}

func (b *Brk) CreateTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[topic]; ok {
		return ErrTopicAlreadyExists
	}
	b.topics[topic] = NewTopic(topic)
	return nil

}

func (b *Brk) DeleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; ok {
		delete(b.topics, topic)
		return nil
	}
	return ErrTopicNotFound
}

func (b *Brk) Topics() []*Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	var topics []*Topic
	for _, topic := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (b *Brk) Subscribe(topicName string) (*Subscriber, error) {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return nil, ErrTopicNotFound
	}

	subID := fmt.Sprintf("sub-%d", time.Now().UnixNano())
	sub := &Subscriber{
		ID: subID,
		Ch: make(chan *Message, 100),
	}

	topic.mu.Lock()
	topic.subscribers[sub.ID] = sub
	topic.mu.Unlock()

	return sub, nil

	





}

