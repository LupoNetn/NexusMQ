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

