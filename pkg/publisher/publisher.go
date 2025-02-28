package publisher

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

type Publisher struct {
	client *pubsub.Client
}

func NewPublisher(projectID string) (*Publisher, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %v", err)
	}
	return &Publisher{
		client: client,
	}, nil
}

func (p *Publisher) Close() error {
	return p.client.Close()
}

func (p *Publisher) ListTopics(ctx context.Context) ([]*pubsub.Topic, error) {
	var topics []*pubsub.Topic
	it := p.client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list topics: %v", err)
		}
		topics = append(topics, topic)
	}
	return topics, nil
}

func (p *Publisher) Publish(ctx context.Context, topicID string, messageData []byte) (string, error) {
	topic := p.client.Topic(topicID)

	// Check if the topic exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to check if topic exists: %v", err)
	}
	if !exists {
		return "", fmt.Errorf("topic %s does not exist", topicID)
	}

	// Publish the message
	result := topic.Publish(ctx, &pubsub.Message{
		Data: messageData,
	})

	// Get the message ID
	msgID, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to publish message: %v", err)
	}

	return msgID, nil
}

func (p *Publisher) PublishFromFile(ctx context.Context, topicID string, filePath string) (string, error) {
	// Read message data from file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read message file: %v", err)
	}

	return p.Publish(ctx, topicID, data)
}

func (p *Publisher) CreateTopic(ctx context.Context, topicID string) (*pubsub.Topic, error) {
	// Check if the topic already exists
	topic := p.client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic exists: %v", err)
	}
	if exists {
		return nil, fmt.Errorf("topic %s already exists", topicID)
	}

	// Create the topic
	topic, err = p.client.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic: %v", err)
	}

	return topic, nil
}
