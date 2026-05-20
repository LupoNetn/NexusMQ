# NexusMQ - Next Steps & Roadmap

This document outlines the remaining tasks to transform the current codebase into a production-ready, lightweight, in-process event broker.

## 🔴 1. The "Must-Haves" (Critical for v1.0)

- [x] **Implement `Unsubscribe` Method**
  - **Why:** To prevent memory leaks when subscribers are no longer needed.
  - **Action:** Add `Unsubscribe(topicName string, subID string) error` to the `Broker` interface. Remove the subscriber from the map and safely `close(sub.Ch)`.

- [x] **Implement Graceful Shutdown (`Shutdown` Method)**
  - **Why:** To cleanly shut down the broker and close all channels when the main Go service stops.
  - **Action:** Add `Close() error` to the `Broker` interface. Loop through all topics and their subscribers, closing all channels.

- [x] **Fix `DeleteTopic` Implementation**
  - **Why:** Currently, deleting a topic just removes it from the map, leaving existing subscribers hanging forever.
  - **Action:** Update `DeleteTopic` to iterate over all subscribers of the topic being deleted and close their channels before removing the topic from the map.

## 🟡 2. The "Should-Haves" (For Robustness & Customization)

- [ ] **Dynamic Channel Buffers**
  - **Why:** Different events have different throughput requirements; hardcoding a buffer of `100` isn't flexible.
  - **Action:** Add `SubscribeWithOpts(topicName string, bufferSize int)` or modify `Subscribe` to accept a buffer size.

- [ ] **Configurable Publish Timeouts**
  - **Why:** A hardcoded 1-second timeout might block high-throughput publishers for too long.
  - **Action:** Accept a `context.Context` in `Publish` or configure timeouts via a `BrokerOptions` struct during `NewBroker()`.

## 🟢 3. The "Nice-to-Haves" (Future Enhancements)

- [ ] **Wildcard Subscriptions**
  - **Why:** Allows users to subscribe to patterns (e.g., `user.*`) rather than just exact matches.
- [ ] **Event Replay / Retries**
  - **Why:** Basic retry logic for transient subscriber failures without needing an external queue.
