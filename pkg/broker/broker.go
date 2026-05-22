package broker

import (
	"fmt"
	"sync"
	"time"
)

func NewBroker() Broker {
	return &Brk{
		mu:     sync.RWMutex{},
		once:   sync.Once{},
		topics: make(map[string]*Topic),
	}
}

func NewTopic(name string) *Topic {
	return &Topic{
		mu:          sync.RWMutex{},
		name:        name,
		subscribers: make(map[string]*subscription),
	}
}

func (b *Brk) CreateTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.shutdown {
		return ErrBrokerShutdown
	}

	if _, ok := b.topics[topic]; ok {
		return ErrTopicAlreadyExists
	}
	b.topics[topic] = NewTopic(topic)
	return nil

}

func (b *Brk) DeleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.shutdown {
		return ErrBrokerShutdown
	}

	t, ok := b.topics[topic]
	if ok {
		t.mu.Lock()
		for _, sub := range t.subscribers {
			close(sub.Ch)
		}
		t.mu.Unlock()

		delete(b.topics, topic)
		return nil
	}
	return ErrTopicNotFound
}

func (b *Brk) Topics() []*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var topics []*Topic
	for _, topic := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (b *Brk) Subscribe(topicName string) (Subscriber, error) {
	b.mu.RLock()
	if b.shutdown {
		b.mu.RUnlock()
		return nil, ErrBrokerShutdown
	}
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return nil, ErrTopicNotFound
	}

	subID := fmt.Sprintf("sub-%d", time.Now().UnixNano())
	sub := &subscription{
		ID: subID,
		Ch: make(chan *Message, 100),
	}

	topic.mu.Lock()
	topic.subscribers[sub.ID] = sub
	topic.mu.Unlock()

	return sub, nil
}

func (b *Brk) Unsubscribe(topicName string, subID string) error {
	b.mu.RLock()
	if b.shutdown {
		b.mu.RUnlock()
		return ErrBrokerShutdown
	}
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return ErrTopicNotFound
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	sub, exists := topic.subscribers[subID]
	if !exists {
		return nil 
	}

	close(sub.Ch)
	delete(topic.subscribers, subID)

	return nil
}

func (b *Brk) Publish(topicName string, msg *Message) error {
	b.mu.RLock()
	if b.shutdown {
		b.mu.RUnlock()
		return ErrBrokerShutdown
	}
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return ErrTopicNotFound
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	var overflowErr error

	for _, sub := range topic.subscribers {
		select {
		case sub.Ch <- msg:
			// successfully delivered
		default:
			// Capture the first overflow error, but continue to try other subscribers
			if overflowErr == nil {
				overflowErr = &OverflowError{
					SubscriberID: sub.ID,
					Topic:        topicName,
					DroppedCount: 1,
				}
			}
		}
	}

	return overflowErr
}

func (b *Brk) Shutdown() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.shutdown = true

		for _, topic := range b.topics {
			topic.mu.Lock()
			for _, sub := range topic.subscribers {
				close(sub.Ch)
			}
			topic.mu.Unlock()
		}

		b.topics = make(map[string]*Topic)
		b.mu.Unlock()
	})

	return nil
}

func (s *subscription) Receive() (*Message, error) {
	msg, ok := <-s.Ch
	if !ok {
		return nil, fmt.Errorf("subscription closed")
	}
	return msg, nil
}
