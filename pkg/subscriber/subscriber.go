package subscriber

import (
	"context"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	client *pubsub.Client
}

func NewSubscriber(projectID string) (*Subscriber, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %v", err)
	}
	return &Subscriber{
		client: client,
	}, nil
}

func (s *Subscriber) Close() error {
	return s.client.Close()
}

func (s *Subscriber) CreateSubscription(ctx context.Context, topicID string, subscriptionID string, output io.Writer) error {
	// Get the topic
	topic := s.client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if topic exists: %v", err)
	}
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicID)
	}

	// Check if subscription already exists
	sub := s.client.Subscription(subscriptionID)
	exists, err = sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if subscription exists: %v", err)
	}
	if exists {
		return fmt.Errorf("subscription %s already exists", subscriptionID)
	}

	// Create the subscription
	_, err = s.client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		return fmt.Errorf("failed to create subscription: %v", err)
	}

	fmt.Fprintf(output, "Subscription %s created for topic %s\n", subscriptionID, topicID)
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, subscriptionID string, shouldAck bool, output io.Writer) error {
	// Check if the subscription exists
	sub := s.client.Subscription(subscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if subscription exists: %v", err)
	}
	if !exists {
		return fmt.Errorf("subscription %s does not exist", subscriptionID)
	}

	// Create a channel to handle errors
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)

	// Start receiving messages
	fmt.Fprintf(output, "Listening for messages on subscription %s...\n", subscriptionID)
	fmt.Fprintf(output, "Press Ctrl+C to exit\n\n")

	go func() {
		defer wg.Done()
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			fmt.Fprintf(output, "Received message: ID=%s\n", msg.ID)
			fmt.Fprintf(output, "Data: %s\n", string(msg.Data))

			if len(msg.Attributes) > 0 {
				fmt.Fprintf(output, "Attributes:\n")
				for key, value := range msg.Attributes {
					fmt.Fprintf(output, "  %s: %s\n", key, value)
				}
			}

			fmt.Fprintf(output, "PublishTime: %v\n\n", msg.PublishTime)

			if shouldAck {
				msg.Ack()
				fmt.Fprintf(output, "Message acknowledged: ID=%s\n\n", msg.ID)
			} else {
				fmt.Fprintf(output, "Message not acknowledged (--ack flag not provided)\n\n")
			}
		})
		if err != nil {
			errCh <- fmt.Errorf("receive error: %v", err)
		}
	}()

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		fmt.Fprintf(output, "\nSubscription stopped: %v\n", ctx.Err())
	case err := <-errCh:
		return err
	}

	wg.Wait()
	return nil
}
