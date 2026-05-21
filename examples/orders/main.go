package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luponetn/nexusmq/internals/broker"
)

func main() {
	fmt.Println("🚀 Starting NexusMQ Real-World Workflow...")

	// 1. Initialize the Broker
	b := broker.NewBroker()
	defer b.Shutdown()

	// 2. Create topics
	b.CreateTopic("orders.created")
	b.CreateTopic("system.logs")

	// 3. Set up a worker to handle emails
	setupEmailWorker(b)

	// 4. Set up a worker to handle database writes
	setupDatabaseWorker(b)

	// 5. Start a Publisher simulating incoming web requests
	go simulateWebTraffic(b)

	// 6. Wait for an interrupt signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down gracefully...")
}

func setupEmailWorker(b broker.Broker) {
	sub, err := b.Subscribe("orders.created")
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, err := sub.Receive()
			if err != nil {
				fmt.Println("[Email Worker] Shut down.")
				return
			}
			fmt.Printf("[Email Worker] 📧 Sending confirmation email for: %s\n", string(msg.Payload))
			
			// Simulate work
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func setupDatabaseWorker(b broker.Broker) {
	sub, err := b.Subscribe("orders.created")
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, err := sub.Receive()
			if err != nil {
				fmt.Println("[Database Worker] Shut down.")
				return
			}
			fmt.Printf("[Database Worker] 💾 Saving to DB: %s (Time: %s)\n", string(msg.Payload), msg.Timestamp.Format("15:04:05"))
		}
	}()
}

func simulateWebTraffic(b broker.Broker) {
	orderID := 1000

	for {
		// Simulate random traffic spikes
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		orderData := fmt.Sprintf("Order_#%d", orderID)
		msg := &broker.Message{
			Payload:   []byte(orderData),
			Timestamp: time.Now(),
		}

		err := b.Publish("orders.created", msg)
		if err != nil {
			fmt.Printf("[Web Server] ⚠️ Failed to publish %s: %v\n", orderData, err)
		} else {
			fmt.Printf("[Web Server] ✅ Published %s\n", orderData)
		}

		orderID++
	}
}
