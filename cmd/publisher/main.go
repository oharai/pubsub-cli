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
