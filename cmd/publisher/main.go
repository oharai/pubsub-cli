package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/GarupanOjisan/pubsub-cli/pkg/publisher"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "pubsub-cli",
		Usage: "A CLI for Google Cloud Pub/Sub",
		Commands: []*cli.Command{
			{
				Name:  "publisher",
				Usage: "Publisher commands",
				Subcommands: []*cli.Command{
					{
						Name:  "topic",
						Usage: "Topic commands",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "project",
								Usage:    "Google Cloud project ID",
								Required: true,
							},
						},
						Action: func(c *cli.Context) error {
							projectID := c.String("project")
							return listTopics(c.Context, projectID)
						},
					},
					{
						Name:  "publish",
						Usage: "Publish a message to a topic",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "project",
								Usage:    "Google Cloud project ID",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "topic",
								Usage:    "Topic ID to publish to",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "message-file",
								Usage:    "Path to the file containing the message to publish",
								Required: true,
							},
						},
						Action: func(c *cli.Context) error {
							projectID := c.String("project")
							topicID := c.String("topic")
							messageFile := c.String("message-file")
							return publishMessage(c.Context, projectID, topicID, messageFile)
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func listTopics(ctx context.Context, projectID string) error {
	// Create a publisher client
	pub, err := publisher.NewPublisher(projectID)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %v", err)
	}
	defer pub.Close()

	// List all topics in the project
	fmt.Printf("Topics in project %s:\n", projectID)
	topics, err := pub.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %v", err)
	}

	// Print the topics
	for _, topic := range topics {
		fmt.Printf("- %s\n", topic.ID())
	}

	if len(topics) == 0 {
		fmt.Println("No topics found in this project.")
	}

	return nil
}

func publishMessage(ctx context.Context, projectID string, topicID string, messageFile string) error {
	// Create a publisher client
	pub, err := publisher.NewPublisher(projectID)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %v", err)
	}
	defer pub.Close()

	// Publish the message from the file
	msgID, err := pub.PublishFromFile(ctx, topicID, messageFile)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	fmt.Printf("Message published to topic %s with ID: %s\n", topicID, msgID)
	return nil
}
