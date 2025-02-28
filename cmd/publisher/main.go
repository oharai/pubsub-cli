package main

import (
	"context"
	"log"
	"os"

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
	return nil
}
