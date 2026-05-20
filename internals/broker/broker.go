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

	t, ok := b.topics[topic]
	if ok {
		// Lock the topic before modifying its subscribers
		t.mu.Lock()
		for _, sub := range t.subscribers {
			close(sub.Ch)
		}
		// We don't need to delete them from the map individually because 
		// the entire topic map is about to be garbage collected!
		t.mu.Unlock()

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

func (b *Brk) Unsubscribe(topicName string, subID string) error {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return ErrTopicNotFound
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	// Find the subscriber
	sub, exists := topic.subscribers[subID]
	if !exists {
		return nil // Already unsubscribed or doesn't exist
	}

	// Close the channel safely and remove from the map
	close(sub.Ch)
	delete(topic.subscribers, subID)

	return nil
}

func (b *Brk) Publish(topicName string, msg *Message) error {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
    b.mu.RUnlock()

	if !ok {
		return ErrTopicNotFound
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	errCh := make(chan error, len(topic.subscribers))
	var wg sync.WaitGroup

	for _,sub := range topic.subscribers {
		wg.Add(1)
		go func(s *Subscriber){
			defer wg.Done()
			select {
			case s.Ch <- msg:
				return
			case <-time.After(1*time.Second):
				fmt.Printf("Subscriber %s timed out\n", s.ID)
				errCh <- fmt.Errorf("%w: %s", ErrPublishTimeout, s.ID)
				return
			}
		}(sub)
	}

	// Wait for all publishes to either succeed or timeout
	wg.Wait()
	close(errCh)

	// If there were any errors, return the first one we find
	for err := range errCh {
		return err
	}

	return nil
}


func (b *Brk) Shutdown() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topic := range b.topics {
		topic.mu.Lock()
		for _, sub := range topic.subscribers {
			close(sub.Ch)
			delete(topic.subscribers, sub.ID)
		}
		topic.mu.Unlock()
	}

	b.topics = make(map[string]*Topic)

	return nil
}

