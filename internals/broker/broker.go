package broker

import (
	"sync"
)

func NewBroker() Broker {
	return &Brk{
		mu:     sync.RWMutex{},
		topics: make(map[string]*Topic),
	}
}

func NewTopic(name string) *Topic {
	return &Topic{
		mu:   sync.RWMutex{},
		name: name,
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
