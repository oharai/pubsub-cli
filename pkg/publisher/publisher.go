package publisher

import (
	"context"
	"fmt"

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
