package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerAtLeastOnce(t *testing.T) {
	ctx := context.Background()

	// Start Redpanda container
	req := testcontainers.ContainerRequest{
		Image:        "docker.redpanda.com/redpandadata/redpanda:v23.3.10",
		ExposedPorts: []string{"9092/tcp"},
		Cmd: []string{
			"redpanda", "start",
			"--mode", "dev-container",
			"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
			"--advertise-kafka-addr", "PLAINTEXT://127.0.0.1:9092",
		},
		WaitingFor: wait.ForLog("Started Kafka API server"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Skipping test because Docker/Redpanda container could not be started: %v", err)
		return
	}
	defer container.Terminate(ctx)

	mappedPort, err := container.MappedPort(ctx, "9092")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host IP: %v", err)
	}

	brokerAddr := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())
	seeds := []string{brokerAddr}
	topic := "test-at-least-once"
	group := "test-group"

	// Produce 5 messages
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatalf("failed to create kgo client: %v", err)
	}
	defer client.Close()

	for i := 1; i <= 5; i++ {
		record := &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		results := client.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			t.Fatalf("failed to produce message %d: %v", i, err)
		}
	}

	// Start consumer with cancellation
	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	consumer, err := NewConsumer(seeds, group, []string{topic})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var processedMessages []string
	var mu sync.Mutex

	handlerErr := make(chan error, 1)
	go func() {
		err := consumer.Start(consumerCtx, func(ctx context.Context, record *kgo.Record) error {
			val := string(record.Value)
			mu.Lock()
			processedMessages = append(processedMessages, val)
			mu.Unlock()

			if val == "message-1" {
				return nil
			}
			if val == "message-2" {
				// Trigger cancellation asynchronously so we don't block
				go cancelConsumer()
				
				// Wait for context cancellation
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return fmt.Errorf("timeout waiting for cancellation")
				}
			}
			return nil
		})
		handlerErr <- err
	}()

	// Wait for consumer to finish/stop
	select {
	case err := <-handlerErr:
		if err != nil && err != context.Canceled {
			t.Logf("consumer stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("test timed out waiting for consumer to stop")
	}

	// Assertions for the first run
	mu.Lock()
	t.Logf("Processed messages in first run: %v", processedMessages)
	if len(processedMessages) < 2 {
		t.Fatalf("expected at least 2 messages to be processed, got %d", len(processedMessages))
	}
	if processedMessages[0] != "message-1" || processedMessages[1] != "message-2" {
		t.Fatalf("unexpected processed messages: %v", processedMessages)
	}
	mu.Unlock()

	// Restart the consumer to verify that message-2 is re-delivered (since its offset should not have been committed)
	restartCtx, cancelRestart := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRestart()

	consumerRestart, err := NewConsumer(seeds, group, []string{topic})
	if err != nil {
		t.Fatalf("failed to create restarted consumer: %v", err)
	}

	var restartedProcessed []string
	err = consumerRestart.Start(restartCtx, func(ctx context.Context, record *kgo.Record) error {
		restartedProcessed = append(restartedProcessed, string(record.Value))
		if len(restartedProcessed) == 4 { // message-2, message-3, message-4, message-5
			cancelRestart()
		}
		return nil
	})
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("restarted consumer failed: %v", err)
	}

	t.Logf("Processed messages in restarted run: %v", restartedProcessed)
	if len(restartedProcessed) == 0 {
		t.Fatal("expected to re-deliver messages, but got none")
	}
	if restartedProcessed[0] != "message-2" {
		t.Fatalf("expected first re-delivered message to be 'message-2', got '%s'", restartedProcessed[0])
	}
}