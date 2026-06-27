package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer wraps the franz-go client and handles the message consumption loop.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer creates a new Consumer instance with auto-commit disabled.
func NewConsumer(seeds []string, group string, topics []string) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), // Ensure auto-commit is disabled to guarantee at-least-once delivery
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kgo client: %w", err)
	}

	return &Consumer{client: client}, nil
}

// HandlerFunc is the callback function for processing messages.
type HandlerFunc func(ctx context.Context, record *kgo.Record) error

// Start starts the consumer loop. It blocks until the context is cancelled or an unrecoverable error occurs.
func (c *Consumer) Start(ctx context.Context, handler HandlerFunc) error {
	defer c.client.Close()

	for {
		// Check if context is cancelled before polling
		if err := ctx.Err(); err != nil {
			return err
		}

		// Poll fetches. We use the main context here.
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf("Fetch error: %v", err.Err)
				if errors.Is(err.Err, context.Canceled) || errors.Is(err.Err, context.DeadlineExceeded) {
					return err.Err
				}
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			// Check if context is cancelled before starting the next handler.
			// This ensures no new messages are processed after cancellation.
			if ctx.Err() != nil {
				break
			}

			record := iter.Next()

			// Process the message. We pass the main context to the handler.
			if err := handler(ctx, record); err != nil {
				log.Printf("Handler failed for record (topic: %s, partition: %d, offset: %d): %v",
					record.Topic, record.Partition, record.Offset, err)
				return fmt.Errorf("handler error: %w", err)
			}

			// Commit offset for this record post-execution.
			// We use a detached context with a timeout to ensure the commit succeeds
			// even if the main context has been cancelled during handler execution.
			commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.client.CommitRecords(commitCtx, record)
			cancel()
			if err != nil {
				log.Printf("Failed to commit offset for record (topic: %s, partition: %d, offset: %d): %v",
					record.Topic, record.Partition, record.Offset, err)
				return fmt.Errorf("commit error: %w", err)
			}
		}
	}
}

func main() {
	fmt.Println("Redpanda Go Consumer Library")
}